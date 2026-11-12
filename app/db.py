"""SQLite 连接管理、表结构、进程注册与启动恢复。

设计要点：
- WAL 模式：多进程（两个 Web 容器）共享同一个数据库文件，读不阻塞写、写不阻塞读。
- 显式事务管理（isolation_level=None），需要写锁时使用 BEGIN IMMEDIATE，
  配合 busy_timeout 让并发写等待而不是立刻报错。
- 地区归属直接落库（sales.region / region_version / unattributed_reason），
  查询时不做任何临时重算。

多进程下的任务归属与启动恢复
----------------------------
每个 Web 进程启动时在 process_registry 注册一个**进程实例**：
instance_id = 主机名-PID-随机 UUID，并由心跳线程持续续期 last_heartbeat。
每个更正任务（correction_job）记录 owner_instance_id，回填线程只执行
归属本实例的任务。

任一进程启动（或容器重启）时执行 recover_stale_jobs()：
- 任务属主仍有新鲜心跳 -> 属主活着，任务/暂存/客户互斥**原样保留**，绝不触碰；
- 属主无注册行、任务无属主（旧库遗留）或心跳超时 -> 属主已被强制终止，
  任务标 failed、清 backfill 时间线与 staging、释放 correction_mutex，
  旧 active 时间线、历史销售归属与已生效修订全部保留，客户可立即重新更正。

PID 在容器/系统重启后可能被复用，因此归属判定使用带随机 UUID 的 instance_id，
两个 PID 相同的实例也能被正确区分。
"""
from __future__ import annotations

import os
import socket
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS region_timeline (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id    TEXT NOT NULL,
    region         TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to   TEXT,
    version        INTEGER NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('active', 'backfill', 'dead')),
    job_id         INTEGER
);
CREATE INDEX IF NOT EXISTS idx_timeline_lookup
    ON region_timeline (customer_id, status, version);

-- 同一客户同时只允许一份进行中的更正：客户级互斥（一行锁，切走/失败即释放）
CREATE TABLE IF NOT EXISTS correction_mutex (
    customer_id TEXT PRIMARY KEY,
    job_id      INTEGER NOT NULL,
    locked_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sales (
    sale_id             TEXT PRIMARY KEY,
    customer_id         TEXT NOT NULL,
    txn_date            TEXT NOT NULL,
    amount_cents        INTEGER NOT NULL,
    region_version      INTEGER NOT NULL,
    region              TEXT,
    unattributed_reason TEXT,
    imported_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales (customer_id, txn_date);
CREATE INDEX IF NOT EXISTS idx_sales_version  ON sales (region_version);

CREATE TABLE IF NOT EXISTS region_revisions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,
    supersedes_version  INTEGER NOT NULL,
    submitted_at        TEXT NOT NULL,
    activated_at        TEXT NOT NULL,
    note                TEXT,
    payload_json        TEXT NOT NULL,
    diff_json           TEXT,
    UNIQUE (customer_id, version)
);

CREATE TABLE IF NOT EXISTS correction_job (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id       TEXT NOT NULL,
    expected_version  INTEGER NOT NULL,
    new_version       INTEGER NOT NULL,
    status            TEXT NOT NULL
                        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    fail_simulation   INTEGER NOT NULL DEFAULT 0,
    backfill_delay_ms INTEGER NOT NULL DEFAULT 0,
    total_sales       INTEGER NOT NULL DEFAULT 0,
    staged_sales      INTEGER NOT NULL DEFAULT 0,
    note              TEXT,
    submitted_at      TEXT NOT NULL,
    started_at        TEXT,
    finished_at       TEXT,
    error             TEXT,
    revision_id       INTEGER,
    owner_instance_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_customer ON correction_job (customer_id, id);

CREATE TABLE IF NOT EXISTS region_staging (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL,
    customer_id  TEXT NOT NULL,
    sale_id      TEXT NOT NULL,
    txn_date     TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    region       TEXT,
    reason       TEXT,
    UNIQUE (job_id, sale_id)
);
CREATE INDEX IF NOT EXISTS idx_staging_job ON region_staging (job_id);

-- 进程实例注册表：每个 Web 进程一行，心跳线程持续刷新 last_heartbeat。
-- instance_id 含随机 UUID，即便两个容器的 PID 相同或 PID 被复用也能区分归属。
CREATE TABLE IF NOT EXISTS process_registry (
    instance_id    TEXT PRIMARY KEY,
    hostname       TEXT NOT NULL,
    pid            INTEGER NOT NULL,
    started_at     TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_registry_heartbeat ON process_registry (last_heartbeat);
"""

# ---------------------------------------------------------------------------
# 进程实例身份与心跳参数（均可由环境变量覆盖，便于演示/测试）
# ---------------------------------------------------------------------------

WORKER_INSTANCE_ID = os.environ.get("WORKER_INSTANCE_ID") or (
    f"{socket.gethostname()}-pid{os.getpid()}-{uuid.uuid4().hex[:12]}"
)
WORKER_HEARTBEAT_MS = int(os.environ.get("WORKER_HEARTBEAT_MS", "1000"))
WORKER_STALE_MS = int(os.environ.get("WORKER_STALE_MS", "5000"))

_heartbeat_started = False
_heartbeat_lock = threading.Lock()


def db_path() -> str:
    return os.environ.get("SALES_DB", "/data/sales.db")


def utcnow() -> str:
    # 毫秒精度：心跳间隔为秒级，毫秒精度足以判断存活
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _heartbeat_age_ms(value: str) -> float:
    return (datetime.now(timezone.utc) - _parse_ts(value)).total_seconds() * 1000.0


def _safe_rollback(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def get_conn() -> sqlite3.Connection:
    """每个调用方（请求线程 / 后台回填线程 / 心跳线程）使用自己的连接。"""
    conn = sqlite3.connect(db_path(), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # 自动提交模式，事务全部由代码显式 BEGIN/COMMIT，避免隐式事务与 IMMEDIATE 冲突。
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ---------------------------------------------------------------------------
# 进程注册与心跳
# ---------------------------------------------------------------------------

def register_worker() -> None:
    """进程启动时登记自身（重启后 instance_id 换新，旧行保留为死亡实例证据）。"""
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        now = utcnow()
        conn.execute(
            "INSERT INTO process_registry (instance_id, hostname, pid, started_at, last_heartbeat) "
            "VALUES (?,?,?,?,?) "
            "ON CONFLICT(instance_id) DO UPDATE SET hostname=excluded.hostname, "
            "pid=excluded.pid, started_at=excluded.started_at, last_heartbeat=excluded.last_heartbeat",
            (WORKER_INSTANCE_ID, socket.gethostname(), os.getpid(), now, now),
        )
        conn.execute("COMMIT")
    except Exception:
        _safe_rollback(conn)
        raise
    finally:
        conn.close()


def heartbeat() -> None:
    """刷新本实例心跳（单条 autocommit 语句，短锁）。"""
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE process_registry SET last_heartbeat=? WHERE instance_id=?",
            (utcnow(), WORKER_INSTANCE_ID),
        )
    finally:
        conn.close()


def start_heartbeat() -> None:
    """启动守护心跳线程；同一进程内幂等（测试中反复建 TestClient 也只起一个）。"""
    global _heartbeat_started
    with _heartbeat_lock:
        if _heartbeat_started:
            return
        _heartbeat_started = True
    thread = threading.Thread(target=_heartbeat_loop, name="worker-heartbeat", daemon=True)
    thread.start()


def _heartbeat_loop() -> None:
    interval = max(0.05, WORKER_HEARTBEAT_MS / 1000.0)
    while True:
        time.sleep(interval)
        try:
            heartbeat()
        except Exception:
            # 单次心跳失败无害：下一轮重试，恢复判定以 stale 超时为准
            pass


def reset_for_tests() -> None:
    """测试夹具专用：清空全部业务表并重新登记当前实例（心跳线程保持运行）。"""
    conn = get_conn()
    try:
        conn.executescript(SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
        for table in ("region_staging", "correction_job", "region_revisions",
                      "sales", "correction_mutex", "region_timeline", "process_registry"):
            conn.execute(f"DELETE FROM {table}")
        now = utcnow()
        conn.execute(
            "INSERT INTO process_registry (instance_id, hostname, pid, started_at, last_heartbeat) "
            "VALUES (?,?,?,?,?)",
            (WORKER_INSTANCE_ID, socket.gethostname(), os.getpid(), now, now),
        )
        conn.execute("COMMIT")
    except Exception:
        _safe_rollback(conn)
        raise
    finally:
        conn.close()
    start_heartbeat()


def is_worker_alive(conn: sqlite3.Connection, instance_id: str | None) -> bool:
    """属主是否存活：有注册行且最近一次心跳未超过 stale 阈值。"""
    if not instance_id:
        return False
    row = conn.execute(
        "SELECT last_heartbeat FROM process_registry WHERE instance_id=?",
        (instance_id,),
    ).fetchone()
    if row is None:
        return False
    return _heartbeat_age_ms(row["last_heartbeat"]) <= WORKER_STALE_MS


def list_workers(conn: sqlite3.Connection) -> list[dict]:
    """返回全部进程实例及存活状态（死亡实例保留用于审计/PID 复用举证）。"""
    rows = conn.execute(
        "SELECT instance_id, hostname, pid, started_at, last_heartbeat "
        "FROM process_registry ORDER BY started_at"
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        age_ms = _heartbeat_age_ms(r["last_heartbeat"])
        out.append({
            "instance_id": r["instance_id"],
            "hostname": r["hostname"],
            "pid": r["pid"],
            "started_at": r["started_at"],
            "last_heartbeat": r["last_heartbeat"],
            "heartbeat_age_ms": int(age_ms),
            "alive": age_ms <= WORKER_STALE_MS,
        })
    return out


# ---------------------------------------------------------------------------
# 建表与启动恢复
# ---------------------------------------------------------------------------

def init_db() -> list[int]:
    """建表/迁移，并执行一次启动恢复，返回被清理的遗留任务 id 列表。

    SQLite 的 COMMIT 是原子的：若切换事务没提交，旧时间线就仍然完好，
    残留的 backfill 行与 staging 行直接清掉、任务标 failed 即可；
    但只有属主已死的任务才能清理——属主存活说明另一进程正在正常回填。
    """
    conn = get_conn()
    try:
        conn.executescript(SCHEMA)
        # 旧库迁移：correction_job 补充属主列（历史任务属主为 NULL，恢复时按遗留处理）
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(correction_job)")}
        if "owner_instance_id" not in cols:
            conn.execute("ALTER TABLE correction_job ADD COLUMN owner_instance_id TEXT")
    finally:
        conn.close()
    return recover_stale_jobs()


def _reap_job(conn: sqlite3.Connection, job_id: int, message: str) -> bool:
    """在一个短写事务内把单个遗留任务标失败并释放其全部占用。返回是否实际清理。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 条件更新：任务已被别人收尾（成功/失败）则一行都不改，撤销与清理都不发生
        cur = conn.execute(
            "UPDATE correction_job SET status='failed', finished_at=?, error=? "
            "WHERE id=? AND status IN ('pending', 'running')",
            (utcnow(), message, job_id),
        )
        if cur.rowcount:
            conn.execute("DELETE FROM region_staging WHERE job_id=?", (job_id,))
            conn.execute(
                "DELETE FROM region_timeline WHERE status='backfill' AND job_id=?",
                (job_id,),
            )
            conn.execute("DELETE FROM correction_mutex WHERE job_id=?", (job_id,))
        conn.execute("COMMIT")
        return bool(cur.rowcount)
    except Exception:
        _safe_rollback(conn)
        raise


def recover_stale_jobs() -> list[int]:
    """启动恢复（也由 /api/admin/recover 手动触发，逻辑完全相同）。

    遍历所有未完成任务：
    - 属主存活（心跳新鲜）-> 跳过，活跃任务、暂存、客户占用保持；
    - 属主死亡/未知/无属主 -> 标 failed 并释放客户，旧归属与修订保留。
    已成功的更正不在扫描范围，恢复与切换交错时不会被撤销；
    其他客户的活跃任务属主不同，也不会被误清。
    """
    reaped: list[int] = []
    conn = get_conn()
    try:
        jobs = conn.execute(
            "SELECT id, owner_instance_id FROM correction_job "
            "WHERE status IN ('pending', 'running') ORDER BY id"
        ).fetchall()
        for job in jobs:
            owner = job["owner_instance_id"]
            if is_worker_alive(conn, owner):
                continue
            if owner:
                message = (
                    "执行进程被强制终止（心跳超时/实例消失），启动恢复标记失败并释放客户；"
                    "旧地区归属与已生效修订保留，可重新提交更正"
                )
            else:
                message = "任务无执行进程归属（旧版本遗留），启动恢复标记失败，旧版本继续生效"
            if _reap_job(conn, job["id"], message):
                reaped.append(job["id"])
    finally:
        conn.close()
    return reaped
