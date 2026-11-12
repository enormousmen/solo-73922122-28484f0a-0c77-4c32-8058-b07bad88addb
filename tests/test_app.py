"""端到端测试：用 TestClient 跑完整 HTTP 链路，数据库指向临时文件。"""
import io
import os
import socket
import threading
import time
import uuid

import pytest

# 必须在导入 app 之前指定数据库路径
_TMP_DB = os.path.join(os.path.dirname(__file__), "_test_sales.db")
os.environ["SALES_DB"] = _TMP_DB

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app import service  # noqa: E402


def _wait_all_jobs(client, timeout=30):
    """等待所有后台回填线程到达终态，避免跨用例的守护线程干扰。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = client.get("/api/corrections").json()["jobs"]
        if all(j["status"] in ("succeeded", "failed") for j in jobs):
            return
        time.sleep(0.05)
    raise AssertionError("存在未结束的回填任务")


@pytest.fixture()
def client():
    # 上一个用例可能残留后台线程，先等它结束再清库
    try:
        with TestClient(app) as c:
            _wait_all_jobs(c)
    except Exception:
        pass
    from app.db import reset_for_tests
    reset_for_tests()
    with TestClient(app) as c:
        yield c
    _wait_all_jobs(c)


def _csv(rows: list[tuple]) -> bytes:
    out = "sale_id,customer_id,txn_date,amount_cents\n"
    out += "\n".join(",".join(str(x) for x in r) for r in rows)
    return out.encode()


def _wait_job(client, job_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = client.get(f"/api/corrections/{job_id}").json()
        if d["status"] in ("succeeded", "failed"):
            return d
        time.sleep(0.05)
    raise AssertionError("job timeout")


def test_full_lifecycle_attribution_gap_and_no_timeline(client):
    # 初始时间线：C1 两段，跨 2026-04-01；C2 空档；C3 无时间线
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "华东", "effective_from": "2026-01-01", "effective_to": "2026-04-01"},
            {"region": "华南", "effective_from": "2026-04-01", "effective_to": None},
        ], "backfill_delay_ms": 0})
    assert r.status_code == 200
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    r = client.post("/api/corrections", json={
        "customer_id": "C2", "expected_version": 0,
        "entries": [
            {"region": "华北", "effective_from": "2026-01-01", "effective_to": "2026-05-01"},
            {"region": "西南", "effective_from": "2026-06-01", "effective_to": None},
        ], "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    rows = [
        ("S1", "C1", "2026-03-31", 1000),   # 华东
        ("S2", "C1", "2026-04-01", 2000),   # 华南（边界：左闭右开）
        ("S3", "C2", "2026-05-15", 3000),   # 空档未归属
        ("S4", "C2", "2026-06-01", 4000),   # 西南
        ("S5", "C3", "2026-07-01", 5000),   # 无时间线
    ]
    r = client.post("/api/sales/import",
                    files={"file": ("s.csv", io.BytesIO(_csv(rows)), "text/csv")})
    assert r.status_code == 200
    assert r.json()["inserted"] == 5

    sales = {s["sale_id"]: s for s in client.get("/api/sales").json()["items"]}
    assert sales["S1"]["region"] == "华东" and sales["S1"]["region_version"] == 1
    assert sales["S2"]["region"] == "华南"
    assert sales["S3"]["region"] is None and sales["S3"]["unattributed_reason"] == "gap"
    assert sales["S4"]["region"] == "西南"
    assert sales["S5"]["region"] is None and sales["S5"]["unattributed_reason"] == "no_timeline"

    # 重复导入幂等
    r = client.post("/api/sales/import",
                    files={"file": ("s.csv", io.BytesIO(_csv(rows)), "text/csv")})
    assert r.json()["skipped_duplicate"] == 5


def test_retroactive_correction_backfills_history(client):
    client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "华东", "effective_from": "2026-01-01", "effective_to": "2026-04-01"},
            {"region": "华南", "effective_from": "2026-04-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    # 取 job 并等待
    job = client.get("/api/corrections?customer_id=C1").json()["jobs"][0]
    assert _wait_job(client, job["id"])["status"] == "succeeded"

    client.post("/api/sales/import", files={"file": ("s.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-03-31", 1000), ("S2", "C1", "2026-05-01", 2000)])), "text/csv")})

    # 追溯更正：生效日提前到 2026-03-01
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": [
            {"region": "华东", "effective_from": "2026-01-01", "effective_to": "2026-03-01"},
            {"region": "华南", "effective_from": "2026-03-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    sales = {s["sale_id"]: s for s in client.get("/api/sales?customer_id=C1").json()["items"]}
    assert sales["S1"]["region"] == "华南" and sales["S1"]["region_version"] == 2
    assert sales["S2"]["region"] == "华南" and sales["S2"]["region_version"] == 2

    # 修订记录携带 diff，且查询返回同版时间线快照
    revs = client.get("/api/revisions/C1").json()["revisions"]
    assert revs[-1]["version"] == 2 and revs[-1]["supersedes_version"] == 1
    assert any(d["op"] == "added" and d["effective_from"] == "2026-03-01" for d in revs[-1]["diff"])
    detail = client.get("/api/sales/S1").json()
    assert all(t["version"] == 2 for t in detail["timeline_snapshot"])
    assert client.get("/api/consistency").json()["consistent"] is True


def test_overlap_rejected_and_half_open_boundary(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "A", "effective_from": "2026-01-01", "effective_to": "2026-03-01"},
            {"region": "B", "effective_from": "2026-02-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert r.status_code == 400 and "重叠" in r.json()["detail"]

    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "A", "effective_from": "2026-03-01", "effective_to": "2026-03-01"}],
        "backfill_delay_ms": 0})
    assert r.status_code == 400

    # 背靠背区间合法：[01-01,03-01) + [03-01,...)，边界日归后者
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "A", "effective_from": "2026-01-01", "effective_to": "2026-03-01"},
            {"region": "B", "effective_from": "2026-03-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert r.status_code == 200
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"


def test_stale_version_rejected(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "A", "effective_from": "2026-01-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    # 当前已为 v1，仍携带 v0 提交 -> 过期拒绝
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "B", "effective_from": "2026-01-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "stale_version"


def test_concurrent_correction_only_one_succeeds(client):
    barrier = threading.Barrier(2)
    results = []

    def submit(delay_ms):
        barrier.wait()
        r = client.post("/api/corrections", json={
            "customer_id": "C1", "expected_version": 0,
            "entries": [{"region": f"R{delay_ms}",
                         "effective_from": "2026-01-01", "effective_to": None}],
            "backfill_delay_ms": delay_ms})
        results.append(r.status_code)

    t1 = threading.Thread(target=submit, args=(1500,))
    t2 = threading.Thread(target=submit, args=(1200,))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(results) == [200, 409]

    # 等两份任务都结束：成功的那份会跑完，被拒那份根本没建任务
    for j in client.get("/api/corrections?customer_id=C1").json()["jobs"]:
        _wait_job(client, j["id"])
    tl = client.get("/api/timeline/C1").json()
    assert tl["version"] == 1 and tl["backfill"] == []


def test_imports_during_backfill_are_included_in_cutover(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "华东", "effective_from": "2026-01-01",
                     "effective_to": "2026-04-01"},
                    {"region": "华南", "effective_from": "2026-04-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100)])), "text/csv")})

    # 带延迟的更正，回填进行中插入新销售
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": [{"region": "华北", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 1500})
    job_id = r.json()["job_id"]
    time.sleep(0.4)
    # 回填期间仍可导入（不被写锁阻塞）
    t0 = time.time()
    r2 = client.post("/api/sales/import", files={"file": ("b.csv", io.BytesIO(_csv([
        ("S2", "C1", "2026-02-10", 200)])), "text/csv")})
    assert r2.status_code == 200
    assert time.time() - t0 < 5  # 导入没有被回填阻塞

    job = _wait_job(client, job_id)
    assert job["status"] == "succeeded" and job["total_sales"] == 2
    sales = {s["sale_id"]: s for s in client.get("/api/sales?customer_id=C1").json()["items"]}
    assert sales["S1"]["region"] == "华北" and sales["S1"]["region_version"] == 2
    assert sales["S2"]["region"] == "华北" and sales["S2"]["region_version"] == 2


def test_failed_correction_keeps_old_version(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "A", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100)])), "text/csv")})

    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": [{"region": "B", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0, "fail_simulation": True})
    job = _wait_job(client, r.json()["job_id"])
    assert job["status"] == "failed" and job["error"]

    tl = client.get("/api/timeline/C1").json()
    assert tl["version"] == 1 and tl["entries"][0]["region"] == "A"
    sales = client.get("/api/sales").json()["items"]
    assert sales[0]["region"] == "A" and sales[0]["region_version"] == 1
    # 失败后允许重新提交（互斥已释放）
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": [{"region": "B", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    assert r.status_code == 200
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"


def test_persistence_across_restart(client, tmp_path):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "A", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    _wait_job(client, r.json()["job_id"])
    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100)])), "text/csv")})

    # 模拟重启：重新 init_db（恢复逻辑）并新起一个 TestClient
    from app.db import init_db
    init_db()
    with TestClient(app) as c2:
        sales = c2.get("/api/sales").json()["items"]
        assert sales[0]["region"] == "A"
        assert c2.get("/api/timeline/C1").json()["version"] == 1
        assert c2.get("/api/consistency").json()["consistent"] is True


def test_json_import_endpoint(client):
    r = client.post("/api/sales/import-json", json={"rows": [
        {"sale_id": "J1", "customer_id": "C9", "txn_date": "2026-01-05", "amount_cents": 123}]})
    assert r.status_code == 200 and r.json()["inserted"] == 1
    item = client.get("/api/sales").json()["items"][0]
    assert item["sale_id"] == "J1" and item["unattributed_reason"] == "no_timeline"


def test_gap_reason_normalized_after_cutover(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C2", "expected_version": 0,
        "entries": [
            {"region": "华北", "effective_from": "2026-01-01", "effective_to": "2026-05-01"},
            {"region": "西南", "effective_from": "2026-06-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    _wait_job(client, r.json()["job_id"])
    client.post("/api/sales/import", files={"file": ("g.csv", io.BytesIO(_csv([
        ("G1", "C2", "2026-05-15", 1)])), "text/csv")})
    # 再做一次更正触发回填，staging_gap 切换后必须归一为 gap
    r = client.post("/api/corrections", json={
        "customer_id": "C2", "expected_version": 1,
        "entries": [
            {"region": "华北", "effective_from": "2026-01-01", "effective_to": "2026-05-01"},
            {"region": "西南", "effective_from": "2026-06-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    _wait_job(client, r.json()["job_id"])
    item = [s for s in client.get("/api/sales").json()["items"] if s["sale_id"] == "G1"][0]
    assert item["region"] is None and item["unattributed_reason"] == "gap"
    assert client.get("/api/consistency").json()["consistent"] is True


def test_bad_csv_rejected_entirely(client):
    r = client.post("/api/sales/import", files={"file": ("bad.csv", io.BytesIO(
        b"sale_id,customer_id,txn_date,amount_cits\nX,C1,2026-01-01,100"), "text/csv")})
    assert r.status_code == 400
    r = client.post("/api/sales/import", files={"file": ("bad.csv", io.BytesIO(
        b"sale_id,customer_id,txn_date,amount_cents\nX,C1,2026-01-01,abc"), "text/csv")})
    assert r.status_code == 400
    assert client.get("/api/sales").json()["total"] == 0


# ---------------------------------------------------------------------------
# 启动恢复：双进程归属、强制终止、PID 复用
# ---------------------------------------------------------------------------

def _timeline(region="R"):
    return [{"region": region, "effective_from": "2026-01-01", "effective_to": None}]


def test_inflight_job_survives_other_process_startup(client):
    """另一容器启动/本进程重启时，属主心跳仍新鲜，活跃任务/暂存/客户占用全部保留。"""
    from app import db
    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100), ("S2", "C1", "2026-02-10", 200)])), "text/csv")})
    # 一个带延迟的回填，让任务停留在 running
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": _timeline("华东"), "backfill_delay_ms": 2000})
    job_id = r.json()["job_id"]
    deadline = time.time() + 5
    while time.time() < deadline:
        d = client.get(f"/api/corrections/{job_id}").json()
        if d["status"] == "running":
            break
        time.sleep(0.02)
    assert d["status"] == "running"

    # 另一个 Web 进程启动：执行与启动时完全相同的恢复逻辑
    reaped = db.recover_stale_jobs()
    assert reaped == []

    d = client.get(f"/api/corrections/{job_id}").json()
    assert d["status"] == "running"
    # running 期间 staging 尚未批量写入，但 backfill 时间线与客户占用必须保留
    conn = db.get_conn()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM correction_mutex WHERE job_id=?", (job_id,)
        ).fetchone()["c"] == 1
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM region_timeline WHERE status='backfill' AND job_id=?",
            (job_id,)).fetchone()["c"] >= 1
    finally:
        conn.close()

    # 回填最终仍能成功完成
    assert _wait_job(client, job_id)["status"] == "succeeded"
    assert client.get("/api/consistency").json()["consistent"] is True


def test_killed_worker_job_is_reaped_and_mutex_released(client):
    """执行回填的进程被强制终止：后续任一进程启动时清理遗留任务、标 failed、释放客户。"""
    from app import db
    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100)])), "text/csv")})
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": _timeline("旧地区"), "backfill_delay_ms": 0})
    _wait_job(client, r.json()["job_id"])

    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": _timeline("新地区"), "backfill_delay_ms": 1500})
    job_id = r.json()["job_id"]
    deadline = time.time() + 5
    while time.time() < deadline:
        d = client.get(f"/api/corrections/{job_id}").json()
        if d["status"] == "running":
            break
        time.sleep(0.02)

    # 模拟 kill -9：线程消失 + 属主注册行被删除（进程没机会注销则心跳超时，这里直接删行）
    conn = db.get_conn()
    try:
        owner = conn.execute(
            "SELECT owner_instance_id FROM correction_job WHERE id=?", (job_id,)
        ).fetchone()["owner_instance_id"]
        conn.execute("DELETE FROM process_registry WHERE instance_id=?", (owner,))
    finally:
        conn.close()

    # 任一 Web 进程启动恢复（手动调用与 startup 同一条路径）
    reaped = db.recover_stale_jobs()
    assert reaped == [job_id]

    d = client.get(f"/api/corrections/{job_id}").json()
    assert d["status"] == "failed" and d["error"]
    # 暂存、backfill 时间线、客户互斥全部清掉
    assert d["staging_preview"] == []
    conn = db.get_conn()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM correction_mutex WHERE customer_id='C1'"
        ).fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM region_timeline WHERE status='backfill'"
        ).fetchone()["c"] == 0
        # 原归属保留：销售仍挂旧版 v1 旧地区
        sale = conn.execute("SELECT * FROM sales WHERE sale_id='S1'").fetchone()
        assert sale["region"] == "旧地区" and sale["region_version"] == 1
        # 已生效修订保留
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM region_revisions WHERE customer_id='C1'"
        ).fetchone()["c"] == 1
    finally:
        conn.close()

    # 客户已释放，可立即重新更正
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": _timeline("重新更正地区"), "backfill_delay_ms": 0})
    assert r.status_code == 200
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"
    sale = client.get("/api/sales/S1").json()
    assert sale["region"] == "重新更正地区" and sale["region_version"] == 2
    assert client.get("/api/consistency").json()["consistent"] is True


def test_recovery_does_not_touch_other_active_jobs_or_succeeded_jobs(client):
    """恢复与切换交错：已成功的更正不撤销，其他客户的活跃任务不被误清。

    死亡任务归属一个同 PID、不同 UUID 的独立实例（模拟另一容器被 kill），
    与当前实例的活跃任务并存，验证恢复只按实例身份而非 PID 判定。
    """
    from app import db
    # C1 成功的更正
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": _timeline("C1地区"), "backfill_delay_ms": 0})
    ok_job = r.json()["job_id"]
    _wait_job(client, ok_job)

    # C2 当前实例的活跃延迟回填
    client.post("/api/sales/import", files={"file": ("c2.csv", io.BytesIO(_csv([
        ("S2", "C2", "2026-01-10", 200)])), "text/csv")})
    r = client.post("/api/corrections", json={
        "customer_id": "C2", "expected_version": 0,
        "entries": _timeline("C2地区"), "backfill_delay_ms": 2500})
    live_job = r.json()["job_id"]
    deadline = time.time() + 5
    while time.time() < deadline:
        if client.get(f"/api/corrections/{live_job}").json()["status"] == "running":
            break
        time.sleep(0.02)
    assert client.get(f"/api/corrections/{live_job}").json()["status"] == "running"

    # C3 的任务归属另一个同 PID、不同 UUID 的实例，且心跳已超时（被强制终止）
    ghost = f"{socket.gethostname()}-pid{os.getpid()}-ghost{uuid.uuid4().hex[:8]}"
    now = db.utcnow()
    conn = db.get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO process_registry (instance_id, hostname, pid, started_at, last_heartbeat) "
            "VALUES (?,?,?,?,?)",
            (ghost, socket.gethostname(), os.getpid(), now,
             "1970-01-01T00:00:00.000+00:00"))
        cur = conn.execute(
            "INSERT INTO correction_job (customer_id, expected_version, new_version, status, "
            "fail_simulation, backfill_delay_ms, submitted_at, owner_instance_id) "
            "VALUES ('C3', 0, 1, 'running', 0, 0, ?, ?)",
            (now, ghost))
        dead_job = cur.lastrowid
        conn.execute(
            "INSERT INTO correction_mutex (customer_id, job_id, locked_at) VALUES ('C3', ?, ?)",
            (dead_job, now))
        conn.execute("COMMIT")
    finally:
        conn.close()

    reaped = db.recover_stale_jobs()
    assert reaped == [dead_job]

    # C2 活跃任务原样保留，C1 成功任务不被撤销
    assert client.get(f"/api/corrections/{live_job}").json()["status"] == "running"
    assert client.get(f"/api/corrections/{ok_job}").json()["status"] == "succeeded"
    assert client.get("/api/timeline/C1").json()["version"] == 1
    assert _wait_job(client, live_job)["status"] == "succeeded"
    assert client.get("/api/consistency").json()["consistent"] is True


def test_zombie_thread_cannot_overwrite_recovery(client):
    """属主被判定死亡并恢复后，旧线程即使醒来继续跑，也无法写回暂存或切换。"""
    from app import db
    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100)])), "text/csv")})
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": _timeline("V1"), "backfill_delay_ms": 0})
    _wait_job(client, r.json()["job_id"])

    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": _timeline("V2"), "backfill_delay_ms": 3000})
    job_id = r.json()["job_id"]
    deadline = time.time() + 5
    while time.time() < deadline:
        if client.get(f"/api/corrections/{job_id}").json()["status"] == "running":
            break
        time.sleep(0.02)

    # 执行进程被 kill，另一进程启动恢复
    conn = db.get_conn()
    try:
        owner = conn.execute(
            "SELECT owner_instance_id FROM correction_job WHERE id=?", (job_id,)
        ).fetchone()["owner_instance_id"]
        conn.execute("DELETE FROM process_registry WHERE instance_id=?", (owner,))
    finally:
        conn.close()
    assert db.recover_stale_jobs() == [job_id]
    assert client.get(f"/api/corrections/{job_id}").json()["status"] == "failed"

    # 旧线程的「僵尸延续」：直接再跑一次同 id 的回填，应被属主守卫拒绝（无副作用）
    service.run_backfill(job_id)
    d = client.get(f"/api/corrections/{job_id}").json()
    assert d["status"] == "failed"
    assert d["staging_preview"] == []
    # 旧归属仍在，版本仍是 v1
    sale = client.get("/api/sales/S1").json()
    assert sale["region"] == "V1" and sale["region_version"] == 1


def test_same_pid_reused_is_distinguished_by_instance_uuid(client):
    """两个实例 PID 相同（容器各自命名空间/PID 复用）：靠 UUID 区分，只回收死属主的任务。"""
    from app import db
    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100), ("S2", "C1", "2026-02-10", 200)])), "text/csv")})
    # 当前实例（心跳新鲜）拥有的活跃任务
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": _timeline("活任务"), "backfill_delay_ms": 2500})
    live_job = r.json()["job_id"]
    deadline = time.time() + 5
    while time.time() < deadline:
        if client.get(f"/api/corrections/{live_job}").json()["status"] == "running":
            break
        time.sleep(0.02)
    assert client.get(f"/api/corrections/{live_job}").json()["status"] == "running"

    # 伪造一个同 PID 但 UUID 不同的旧实例，拥有一个遗留任务
    ghost_instance = f"{socket.gethostname()}-pid{os.getpid()}-deadbeef0000"
    conn = db.get_conn()
    try:
        now = db.utcnow()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO process_registry (instance_id, hostname, pid, started_at, last_heartbeat) "
            "VALUES (?,?,?,?,?)",
            (ghost_instance, socket.gethostname(), os.getpid(), now,
             "1970-01-01T00:00:00.000+00:00"))
        cur = conn.execute(
            "INSERT INTO correction_job (customer_id, expected_version, new_version, status, "
            "fail_simulation, backfill_delay_ms, submitted_at, owner_instance_id) "
            "VALUES ('C2', 0, 1, 'running', 0, 1000, ?, ?)",
            (now, ghost_instance))
        dead_job = cur.lastrowid
        conn.execute(
            "INSERT INTO correction_mutex (customer_id, job_id, locked_at) VALUES ('C2', ?, ?)",
            (dead_job, now))
        conn.execute("COMMIT")
    finally:
        conn.close()

    reaped = db.recover_stale_jobs()
    assert reaped == [dead_job]
    # 同 PID 的活实例任务不受影响
    assert client.get(f"/api/corrections/{live_job}").json()["status"] == "running"
    assert _wait_job(client, live_job)["status"] == "succeeded"
    # 死实例任务被标 failed，C2 互斥释放
    assert client.get(f"/api/corrections/{dead_job}").json()["status"] == "failed"
    conn = db.get_conn()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM correction_mutex WHERE customer_id='C2'"
        ).fetchone()["c"] == 0
    finally:
        conn.close()


def test_admin_endpoints_report_workers_and_recovery(client):
    """管理接口可观测进程实例与手动恢复。"""
    d = client.get("/api/admin/workers").json()
    assert d["current_instance_id"]
    assert any(w["instance_id"] == d["current_instance_id"] and w["alive"]
               for w in d["workers"])
    r = client.post("/api/admin/recover")
    assert r.status_code == 200 and r.json()["reaped_job_ids"] == []
    h = client.get("/api/health").json()
    assert h["instance_id"] == d["current_instance_id"] and h["pid"] > 0
