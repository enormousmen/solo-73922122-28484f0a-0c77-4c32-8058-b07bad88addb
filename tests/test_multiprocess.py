"""多进程集成测试：真实 subprocess 模拟两个 Web 容器共享一个 WAL 数据库。

与 test_app.py 的同进程测试互补，这里验证：
1. 一个进程正在延迟回填，另一个进程启动 -> 活跃任务/暂存/客户占用保持，
   回填最终成功，且期间另一进程新导入的销售在切换时一并切到新版本；
2. 执行回填的进程被 SIGKILL 强制终止 -> 后续任一进程启动清理遗留任务、
   标 failed、释放客户，原归属与已生效修订保留，可重新更正；
3. 两个实例 PID 相同（手工指定同 PID 不同 UUID）也能正确区分归属。

辅助进程（_mp_worker.py）直接调用 app 层函数（init_db/register_worker/
start_heartbeat/submit_correction/run_backfill），与 uvicorn 进程内执行路径一致。
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
WORKER = Path(__file__).resolve().parent / "_mp_worker.py"
MARKER = "@@RESULT@@"


def _worker_env(db_path: str) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["SALES_DB"] = db_path
    env["WORKER_HEARTBEAT_MS"] = "500"
    env["WORKER_STALE_MS"] = "2000"
    return env


def _run(db_path: str, payload: dict, timeout: int = 60) -> dict:
    proc = subprocess.run(
        [sys.executable, "-I", str(WORKER), db_path, json.dumps(payload)],
        capture_output=True, text=True, timeout=timeout, env=_worker_env(db_path),
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"worker failed rc={proc.returncode}\nSTDOUT:{proc.stdout}\nSTDERR:{proc.stderr}")
    return _parse_result(proc.stdout, proc.stderr)


def _parse_result(stdout: str, stderr: str = "") -> dict:
    line = next((ln for ln in stdout.splitlines() if ln.startswith(MARKER)), None)
    if line is None:
        raise AssertionError(f"no result marker\nSTDOUT:{stdout}\nSTDERR:{stderr}")
    return json.loads(line[len(MARKER):])


def _spawn_holder(db_path: str, payload: dict) -> subprocess.Popen:
    """启动一个持续存活、维持心跳并在后台线程跑回填的 worker 进程。"""
    return subprocess.Popen(
        [sys.executable, "-I", str(WORKER), db_path, json.dumps(payload)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env=_worker_env(db_path),
    )


def _holder_job_id(holder: subprocess.Popen, timeout: int = 30) -> int:
    assert holder.stdout is not None
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = holder.stdout.readline()
        if not line:
            if holder.poll() is not None:
                err = holder.stderr.read() if holder.stderr else ""
                raise AssertionError(f"holder exited early: {err}")
            time.sleep(0.05)
            continue
        if line.startswith(MARKER):
            return _parse_result(line)["job_id"]
    raise AssertionError("holder did not report job id")


def _wait_status(db_path: str, job_id: int, wanted: set[str], timeout: int = 40) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = _run(db_path, {"action": "job", "job_id": job_id})["job"]
        if d["status"] in wanted:
            return d
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} did not reach {wanted}")


def test_second_process_startup_preserves_inflight_backfill_and_cutover_includes_new_sales():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "shared.db")
        # 进程 A 启动（建表+注册+心跳）
        assert _run(db_path, {"action": "startup", "instance": "A"})["reaped"] == []
        _run(db_path, {"action": "import", "rows": [
            {"sale_id": "S1", "customer_id": "C1", "txn_date": "2026-01-10", "amount_cents": 100}]})

        # A 的长延迟回填在独立存活进程里运行（12s 窗口足够另一个进程启动/导入）
        holder = _spawn_holder(db_path, {
            "action": "submit", "instance": "A", "customer_id": "C1",
            "expected_version": 0, "delay_ms": 12000, "wait": False, "alive_seconds": 16})
        try:
            job_id = _holder_job_id(holder)
            assert _wait_status(db_path, job_id, {"running"})["status"] == "running"

            # 进程 B 启动：旧实现会把任务误标 failed；新实现必须跳过存活属主
            assert _run(db_path, {"action": "startup", "instance": "B"})["reaped"] == []
            d = _run(db_path, {"action": "job", "job_id": job_id})["job"]
            assert d["status"] == "running" and d["owner_alive"] is True
            state = _run(db_path, {"action": "state", "job_id": job_id})
            assert state["mutex_rows"] == 1 and state["backfill_rows"] >= 1

            # 回填期间由 B 再导入一条新销售（网页导入等价路径）
            assert _run(db_path, {"action": "import", "rows": [
                {"sale_id": "S2", "customer_id": "C1", "txn_date": "2026-02-10",
                 "amount_cents": 200}]})["inserted"] == 1
            # 新销售入仓时仍是旧版本（v0 无时间线 -> no_timeline）
            sale = _run(db_path, {"action": "sale", "sale_id": "S2"})["sale"]
            assert sale["region_version"] == 0 and sale["unattributed_reason"] == "no_timeline"
        finally:
            # 不杀 holder，等它自然完成回填与切换
            holder.wait(timeout=45)

        final = _wait_status(db_path, job_id, {"succeeded", "failed"})
        assert final["status"] == "succeeded"
        # 期间新导入的 S2 也被一起切到新版 v1，历史 S1 同版
        for sid in ("S1", "S2"):
            sale = _run(db_path, {"action": "sale", "sale_id": sid})["sale"]
            assert sale["region_version"] == 1 and sale["region"] == "R1"
        assert _run(db_path, {"action": "consistency"})["consistency"]["consistent"] is True


def test_killed_worker_is_reaped_by_next_startup_and_customer_released():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "shared.db")
        _run(db_path, {"action": "startup", "instance": "A"})
        _run(db_path, {"action": "import", "rows": [
            {"sale_id": "S1", "customer_id": "C1", "txn_date": "2026-01-10", "amount_cents": 100}]})
        # 先建 v1 并切换成功
        r = _run(db_path, {"action": "submit", "instance": "A", "customer_id": "C1",
                           "expected_version": 0, "delay_ms": 0, "wait": True})
        assert _wait_status(db_path, r["job_id"], {"succeeded"})["status"] == "succeeded"

        # v2 长延迟回填；worker 在进入 running 后硬退出（os._exit，等价 kill -9：
        # 无 finally、无 atexit、无任何清理），模拟执行进程被强制终止
        holder = _spawn_holder(db_path, {
            "action": "submit", "instance": "A", "customer_id": "C1",
            "expected_version": 1, "delay_ms": 30000, "wait": False,
            "alive_seconds": 60, "suicide_after_ms": 1500})
        try:
            job_id = _holder_job_id(holder)
            assert _wait_status(db_path, job_id, {"running"})["status"] == "running"
            holder.wait(timeout=30)
            assert holder.returncode == 137
        finally:
            if holder.poll() is None:
                holder.kill()

        # 等属主心跳老化超过 stale 阈值
        time.sleep(2.3)
        # 任一 Web 进程启动恢复
        assert job_id in _run(db_path, {"action": "startup", "instance": "B"})["reaped"]
        d = _run(db_path, {"action": "job", "job_id": job_id})["job"]
        assert d["status"] == "failed" and d["error"]
        state = _run(db_path, {"action": "state", "job_id": job_id})
        assert state["mutex_rows"] == 0 and state["backfill_rows"] == 0 and state["staging_rows"] == 0
        # 原归属与已生效修订保留
        sale = _run(db_path, {"action": "sale", "sale_id": "S1"})["sale"]
        assert sale["region"] == "R1" and sale["region_version"] == 1
        assert len(_run(db_path, {"action": "revisions", "customer_id": "C1"})["revisions"]) == 1
        # 客户已释放，可重新更正并成功
        r = _run(db_path, {"action": "submit", "instance": "B", "customer_id": "C1",
                           "expected_version": 1, "delay_ms": 0, "wait": True,
                           "region": "R2"})
        assert _wait_status(db_path, r["job_id"], {"succeeded", "failed"})["status"] == "succeeded"
        sale = _run(db_path, {"action": "sale", "sale_id": "S1"})["sale"]
        assert sale["region"] == "R2" and sale["region_version"] == 2
        assert _run(db_path, {"action": "consistency"})["consistency"]["consistent"] is True


def test_real_sigkill_is_reaped_when_signals_permitted():
    """真实 SIGKILL 路径（受限 CI/沙箱无信号权限时自动跳过）。"""
    probe = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(5)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.3)
    signals_ok = True
    try:
        os.kill(probe.pid, signal.SIGKILL)
    except PermissionError:
        signals_ok = False
    finally:
        try:
            probe.wait(timeout=5)
        except Exception:
            pass
    if not signals_ok:
        pytest.skip("当前环境不允许向子进程发信号，SIGKILL 路径由 os._exit 用例等价覆盖")

    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "shared.db")
        _run(db_path, {"action": "startup", "instance": "A"})
        _run(db_path, {"action": "import", "rows": [
            {"sale_id": "S1", "customer_id": "C1", "txn_date": "2026-01-10", "amount_cents": 100}]})
        holder = _spawn_holder(db_path, {
            "action": "submit", "instance": "A", "customer_id": "C1",
            "expected_version": 0, "delay_ms": 30000, "wait": False, "alive_seconds": 60})
        try:
            job_id = _holder_job_id(holder)
            assert _wait_status(db_path, job_id, {"running"})["status"] == "running"
            pidfile = Path(db_path + ".pid")
            deadline = time.time() + 10
            while time.time() < deadline and not pidfile.exists():
                time.sleep(0.1)
            os.kill(int(pidfile.read_text().strip()), signal.SIGKILL)
            holder.wait(timeout=10)
            assert holder.returncode is not None and holder.returncode < 0
        finally:
            if holder.poll() is None:
                holder.kill()
        time.sleep(2.3)
        assert job_id in _run(db_path, {"action": "startup", "instance": "B"})["reaped"]
        assert _run(db_path, {"action": "job", "job_id": job_id})["job"]["status"] == "failed"


def test_same_pid_different_uuid_are_distinct_owners():
    """两个实例 PID 相同、UUID 不同：心跳新鲜的任务保留，只有死属主任务被回收。"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "shared.db")
        same_pid = 4242
        live = f"host-pid{same_pid}-{uuid.uuid4().hex[:12]}"
        dead = f"host-pid{same_pid}-{uuid.uuid4().hex[:12]}"
        _run(db_path, {"action": "startup", "instance": live})
        _run(db_path, {"action": "import", "rows": [
            {"sale_id": "S1", "customer_id": "C1", "txn_date": "2026-01-10", "amount_cents": 100}]})

        holder = _spawn_holder(db_path, {
            "action": "submit", "instance": live, "customer_id": "C1",
            "expected_version": 0, "delay_ms": 12000, "wait": False, "alive_seconds": 40})
        try:
            live_job = _holder_job_id(holder)
            assert _wait_status(db_path, live_job, {"running"})["status"] == "running"
            # 死实例（同 PID 不同 UUID，心跳陈旧）的遗留任务
            dead_job = _run(db_path, {"action": "ghost", "instance": dead, "pid": same_pid,
                                      "customer_id": "C2"})["job_id"]

            rec = _run(db_path, {"action": "recover"})
            assert rec["reaped"] == [dead_job]
            assert _run(db_path, {"action": "job", "job_id": live_job})["job"]["status"] == "running"
            assert _run(db_path, {"action": "job", "job_id": dead_job})["job"]["status"] == "failed"
        finally:
            holder.wait(timeout=45)

        # 活实例心跳始终新鲜，其任务最终成功；死实例任务保持 failed 不被翻案
        assert _wait_status(db_path, live_job, {"succeeded", "failed"})["status"] == "succeeded"
        assert _run(db_path, {"action": "job", "job_id": dead_job})["job"]["status"] == "failed"
