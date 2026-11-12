"""多进程集成测试的辅助进程：以真实进程身份执行 app 层动作。

用法：python -I _mp_worker.py <db_path> <json payload>
输出：stdout 打印一行 @@RESULT@@<json>
"""
from __future__ import annotations

import json
import os
import sys
import time

# -I 模式下没有 CWD，显式把仓库根加入 sys.path
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("WORKER_HEARTBEAT_MS", "500")
os.environ.setdefault("WORKER_STALE_MS", "2000")


def _entries(region: str = "R1"):
    return [{"region": region, "effective_from": "2026-01-01", "effective_to": None}]


def main() -> None:
    db_path, raw = sys.argv[1], sys.argv[2]
    payload = json.loads(raw)
    os.environ["SALES_DB"] = db_path
    instance = payload.get("instance")
    if instance:
        os.environ["WORKER_INSTANCE_ID"] = instance

    # 延迟导入，确保环境变量先生效
    from app import db, service  # noqa: E402

    action = payload["action"]
    result: dict = {}

    if action == "startup":
        reaped = db.init_db()
        db.register_worker()
        db.start_heartbeat()
        # 保持进程存活一小段，发出若干次心跳，证明实例新鲜
        time.sleep(1.2)
        result = {"reaped": reaped, "instance": db.WORKER_INSTANCE_ID}

    elif action == "submit":
        db.init_db()
        db.register_worker()
        db.start_heartbeat()
        created = service.submit_correction(
            payload["customer_id"], _entries(payload.get("region", "R1")),
            payload["expected_version"], payload["delay_ms"], False, "mp-test")
        job_id = created["job_id"]
        if payload.get("wait"):
            service.run_backfill(job_id)
            result = {"job_id": job_id}
        else:
            # 后台线程跑回填，主进程继续存活并维持心跳；写 pidfile 供测试强杀
            import threading
            threading.Thread(target=service.run_backfill, args=(job_id,), daemon=True).start()
            pidfile = db_path + ".pid"
            with open(pidfile, "w") as fh:
                fh.write(str(os.getpid()))
            # 立即把 job_id 输出给父进程（行缓冲在管道下不可靠，显式 flush）
            print("@@RESULT@@" + json.dumps({"job_id": job_id}), flush=True)
            if payload.get("suicide_after_ms"):
                # 模拟 kill -9：硬退出，不跑任何 finally/atexit/清理逻辑
                time.sleep(payload["suicide_after_ms"] / 1000.0)
                os._exit(137)
            # 存活到被杀死或到达上限，期间持续心跳
            deadline = time.time() + payload.get("alive_seconds", 45)
            while time.time() < deadline:
                db.heartbeat()
                time.sleep(0.3)
            return  # 结果已输出

    elif action == "import":
        db.init_db()
        result = service.import_sales(payload["rows"])

    elif action == "job":
        db.init_db()
        conn = db.get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM correction_job WHERE id=?", (payload["job_id"],)).fetchone()
            job = dict(row)
            job["owner_alive"] = db.is_worker_alive(conn, job["owner_instance_id"])
        finally:
            conn.close()
        result = {"job": job}

    elif action == "state":
        db.init_db()
        conn = db.get_conn()
        try:
            result = {
                "mutex_rows": conn.execute(
                    "SELECT COUNT(*) AS c FROM correction_mutex WHERE job_id=?",
                    (payload["job_id"],)).fetchone()["c"],
                "backfill_rows": conn.execute(
                    "SELECT COUNT(*) AS c FROM region_timeline WHERE status='backfill' "
                    "AND job_id=?", (payload["job_id"],)).fetchone()["c"],
                "staging_rows": conn.execute(
                    "SELECT COUNT(*) AS c FROM region_staging WHERE job_id=?",
                    (payload["job_id"],)).fetchone()["c"],
            }
        finally:
            conn.close()

    elif action == "sale":
        db.init_db()
        conn = db.get_conn()
        try:
            result = {"sale": dict(conn.execute(
                "SELECT * FROM sales WHERE sale_id=?", (payload["sale_id"],)).fetchone())}
        finally:
            conn.close()

    elif action == "revisions":
        db.init_db()
        conn = db.get_conn()
        try:
            rows = conn.execute(
                "SELECT version FROM region_revisions WHERE customer_id=? ORDER BY version",
                (payload["customer_id"],)).fetchall()
        finally:
            conn.close()
        result = {"revisions": [dict(r) for r in rows]}

    elif action == "consistency":
        db.init_db()
        from fastapi.testclient import TestClient
        from app.main import app
        with TestClient(app) as client:
            result = {"consistency": client.get("/api/consistency").json()}

    elif action == "recover":
        # init_db() 内部已执行一次启动恢复；直接返回其回收结果，避免二次调用恒为空
        result = {"reaped": db.init_db()}

    elif action == "ghost":
        # 手工构造一个同 PID、不同 UUID、心跳陈旧的死亡实例及其遗留任务
        db.init_db()
        conn = db.get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = db.utcnow()
            conn.execute(
                "INSERT INTO process_registry (instance_id, hostname, pid, started_at, last_heartbeat) "
                "VALUES (?,?,?,?,?)",
                # 心跳写为 1970 年（ISO 合法且必超 stale 阈值），表示该实例早已死亡
                (instance, "ghost-host", int(payload["pid"]),
                 "1970-01-01T00:00:00.000+00:00", "1970-01-01T00:00:00.000+00:00"))
            cur = conn.execute(
                "INSERT INTO correction_job (customer_id, expected_version, new_version, status, "
                "fail_simulation, backfill_delay_ms, submitted_at, owner_instance_id) "
                "VALUES (?, 0, 1, 'running', 0, 0, ?, ?)",
                (payload["customer_id"], now, instance))
            job_id = cur.lastrowid
            conn.execute(
                "INSERT INTO correction_mutex (customer_id, job_id, locked_at) VALUES (?,?,?)",
                (payload["customer_id"], job_id, now))
            conn.execute("COMMIT")
        finally:
            conn.close()
        result = {"job_id": job_id}

    else:
        raise SystemExit(f"unknown action {action}")

    print("@@RESULT@@" + json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
