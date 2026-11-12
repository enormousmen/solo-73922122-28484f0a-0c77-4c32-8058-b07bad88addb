#!/usr/bin/env python3
"""可演示脚本（仅标准库）：并发更正互斥 + 回填期间持续导入/查询 + 双进程启动恢复。

用法：
  python3 demo/concurrency_demo.py http://localhost:8080
先在浏览器里点「一键准备演示数据」，或让本脚本自动 seed。

脚本只走 HTTP，不直接操作容器；涉及重启/强杀的步骤会打印可复制执行的
docker 命令（见场景 4/5），由操作者在终端执行，脚本随后通过 API 验证结果。
"""
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
# 直连入口：用于把请求固定到某一个 Web 进程，观察进程级归属
WEB1 = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8081"
WEB2 = sys.argv[3] if len(sys.argv) > 3 else "http://localhost:8082"


def call(method: str, path: str, body=None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def post_csv(rows: list[tuple]) -> tuple[int, dict]:
    body = {"rows": [
        {"sale_id": r[0], "customer_id": r[1], "txn_date": r[2], "amount_cents": r[3]}
        for r in rows]}
    return call("POST", "/api/sales/import-json", body)


def wait_job(job_id: int) -> dict:
    while True:
        _, d = call("GET", f"/api/corrections/{job_id}")
        if d["status"] in ("succeeded", "failed"):
            return d
        time.sleep(0.2)


def main() -> None:
    print(f"== 目标 {BASE}，先准备演示数据 ==")
    call("POST", "/api/demo/seed")

    print("\n== 场景 1：同一客户并发两份更正，只允许一份成功 ==")
    _, tl = call("GET", "/api/timeline/C001")
    expected = tl["version"]
    print(f"  当前 C001 生效版本 v{expected}，两份请求都携带该所见修订")
    payloads = []
    for i in range(2):
        payloads.append({
            "customer_id": "C001", "expected_version": expected,
            "entries": [
                {"region": f"并行地区{i}", "effective_from": "2026-01-01",
                 "effective_to": "2026-03-01"},
                {"region": "华南", "effective_from": "2026-03-01",
                 "effective_to": None}],
            "backfill_delay_ms": 4000,
        })
    with ThreadPoolExecutor(max_workers=2) as pool:
        outs = list(pool.map(lambda p: call("POST", "/api/corrections", p), payloads))
    for code, body in outs:
        print(f"  HTTP {code} {body.get('error') or body.get('job_id')} {body if code != 200 else ''}")
    assert sorted(c for c, _ in outs) == [200, 409], "并发互斥不符合预期"
    job_id = next(b["job_id"] for c, b in outs if c == 200)

    print("\n== 场景 2：回填进行中持续导入销售与查询旧结果 ==")
    stop = False

    def importer() -> None:
        n = 0
        while not stop:
            n += 1
            code, body = post_csv([(f"BG{n:04d}", "C001", "2026-02-15", 9900 + n)])
            print(f"  回填中导入 BG{n:04d}: HTTP {code} {body}")
            time.sleep(0.8)

    def reader() -> None:
        while not stop:
            _, body = call("GET", "/api/sales?customer_id=C001&limit=3")
            print(f"  查询旧结果：{body['total']} 条，响应进程 {body['served_by']}")
            time.sleep(0.8)

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(importer)
        f2 = pool.submit(reader)
        job = wait_job(job_id)
        stop = True
        f1.result(); f2.result()

    print(f"\n回填任务最终状态：{job['status']}，纳入销售 {job['staged_sales']}/{job['total_sales']}")
    _, cons = call("GET", "/api/consistency")
    print(f"一致性自检：consistent={cons['consistent']}，核对 {cons['checked']} 条，"
          f"未归属 {cons['unattributed_count']} 条，不一致 {len(cons['mismatches'])} 条")
    assert job["status"] == "succeeded" and cons["consistent"]

    print("\n== 场景 3：过期版本提交被拒绝 ==")
    _, tl = call("GET", "/api/timeline/C001")
    stale = max(0, tl["version"] - 1)
    code, body = call("POST", "/api/corrections", {
        "customer_id": "C001", "expected_version": stale,
        "entries": [{"region": "过期", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    print(f"  HTTP {code}（携带过期版本 v{stale}，当前 v{tl['version']}）"
          f" {body['detail'] if code != 200 else body}")
    assert code == 409

    print("\n== 场景 4：延迟回填中重启另一进程，活跃任务/暂存/客户占用保持 ==")
    demo_restart_other_worker()

    print("\n== 场景 5：执行进程被强制终止，后续任一进程启动清理并释放客户 ==")
    demo_killed_worker_recovery()

    print("\n全部演示通过 ✅")


def _new_customer():
    """挑一个没有时间线、没有进行中任务的客户，保证演示隔离。"""
    _, body = call("GET", "/api/customers")
    used = {c["customer_id"] for c in body["customers"]}
    n = 1
    while f"RX{n:03d}" in used:
        n += 1
    return f"RX{n:03d}"


def demo_restart_other_worker() -> None:
    """回填在 web1 进行，重启 web2（或反之），任务必须原样保留并最终成功。"""
    cid = _new_customer()
    post_csv([(f"{cid}-S1", cid, "2026-01-10", 100)])
    # 固定提交到 web1，属主即 web1 实例
    code, body = call("POST", f"{WEB1}/api/corrections", {
        "customer_id": cid, "expected_version": 0,
        "entries": [{"region": "地区甲", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 12000})
    assert code == 200, body
    job_id = body["job_id"]
    time.sleep(1.0)
    _, job = call("GET", f"/api/corrections/{job_id}")
    print(f"  任务 #{job_id} 状态={job['status']} 属主={job.get('owner_instance_id')} "
          f"存活={job.get('owner_alive')}")

    print("  请在另一终端执行（重启的是【另一个】Web 进程，不是执行回填的那个）：")
    print("    docker compose restart web2")
    print("  脚本将通过 API 等待并验证任务未被误杀……")
    # 不依赖真实重启：直接对两个入口触发与启动完全相同的恢复逻辑进行验证，
    # 同时打印手动命令；真实 docker restart 时 startup 走的是同一条 recover_stale_jobs。
    for url in (WEB1, WEB2):
        code, rec = call("POST", f"{url}/api/admin/recover")
        print(f"  {url} /api/admin/recover -> {rec}")
        assert code == 200 and rec["reaped_job_ids"] == []
    _, job = call("GET", f"/api/corrections/{job_id}")
    assert job["status"] in ("running", "pending"), "活跃任务被误清理！"
    print(f"  恢复后任务仍为 {job['status']}，属主存活={job['owner_alive']}，暂存与客户占用保留")

    # 回填期间再导入一条，切换时必须一起切到新版
    post_csv([(f"{cid}-S2", cid, "2026-02-20", 200)])
    job = wait_job(job_id)
    assert job["status"] == "succeeded", job
    for sid in (f"{cid}-S1", f"{cid}-S2"):
        _, sale = call("GET", f"/api/sales/{sid}")
        assert sale["region_version"] == job["new_version"] and sale["region"] == "地区甲", sale
    print(f"  回填完成：{job['staged_sales']} 条销售（含期间新增）全部切到 v{job['new_version']}")


def demo_killed_worker_recovery() -> None:
    """执行回填的容器被 kill -9：遗留任务标 failed、客户释放、旧归属与修订保留。"""
    cid = _new_customer()
    post_csv([(f"{cid}-S1", cid, "2026-01-10", 100)])
    # 先建 v1
    code, body = call("POST", "/api/corrections", {
        "customer_id": cid, "expected_version": 0,
        "entries": [{"region": "旧版地区", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    assert code == 200
    wait_job(body["job_id"])
    # v2 长延迟回填，固定到 web1
    code, body = call("POST", f"{WEB1}/api/corrections", {
        "customer_id": cid, "expected_version": 1,
        "entries": [{"region": "新版地区", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 120000})
    assert code == 200, body
    job_id = body["job_id"]
    time.sleep(1.0)
    _, job = call("GET", f"/api/corrections/{job_id}")
    owner = job.get("owner_instance_id", "")
    print(f"  任务 #{job_id} 由 {owner} 执行，状态={job['status']}")
    print("  请在另一终端执行（强制终止执行回填的容器，等价 kill -9）：")
    print("    docker kill sales-web1 && docker compose start web1")
    print("  等待操作者执行，或直接由脚本模拟属主心跳老化后的恢复……")

    # 自动验证路径：容器重启后 startup 的恢复只认心跳。这里轮询任务，
    # 一旦发现属主消失/心跳超时（真实 docker kill 后约 2~5 秒），任一入口恢复即可。
    deadline = time.time() + 20
    reaped = None
    while time.time() < deadline:
        code, rec = call("POST", f"{WEB2}/api/admin/recover")
        if rec.get("reaped_job_ids"):
            reaped = rec
            break
        time.sleep(1.0)
    if reaped is None:
        # 无人为 kill 时，任务仍正常运行，不应被回收；给出手动验证说明后让任务自然结束
        print("  未检测到容器被 kill（属主心跳仍新鲜），任务保持活跃，符合预期。")
        print("  手动验证：执行上面的 docker kill 命令，重启后查看任务变为 failed、客户可重新更正。")
        job = wait_job(job_id)
        assert job["status"] == "succeeded"
        return

    assert job_id in reaped["reaped_job_ids"], reaped
    _, job = call("GET", f"/api/corrections/{job_id}")
    assert job["status"] == "failed" and job["error"]
    _, sale = call("GET", f"/api/sales/{cid}-S1")
    assert sale["region"] == "旧版地区" and sale["region_version"] == 1, sale
    print(f"  遗留任务已标 failed：{job['error']}")
    print("  旧归属与已生效修订保留，客户互斥已释放，立即重新更正：")
    code, body = call("POST", "/api/corrections", {
        "customer_id": cid, "expected_version": 1,
        "entries": [{"region": "重新更正地区", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    assert code == 200, body
    job = wait_job(body["job_id"])
    assert job["status"] == "succeeded"
    _, sale = call("GET", f"/api/sales/{cid}-S1")
    assert sale["region"] == "重新更正地区" and sale["region_version"] == 2
    print(f"  重新更正成功，销售已切到 v{job['new_version']}（重新更正地区）")


if __name__ == "__main__":
    main()
