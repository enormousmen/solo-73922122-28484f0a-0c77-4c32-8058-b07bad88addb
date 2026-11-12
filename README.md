# 销售地区归属 · 追溯更正平台

按**交易发生日**确定客户地区的销售数仓演示系统。地区归属在入仓/回填切换时**入库存储**，
查询不做任何临时重算；支持按客户整套替换地区有效期、乐观修订并发控制、无长写锁的后台回填，
以及两个 Web 进程并发下的原子切换。

技术栈：FastAPI + SQLite（WAL）+ 原生 HTML/JS 前端 + Nginx 负载均衡 + Docker Compose。

## 一键启动

```bash
docker compose up --build
```

浏览器入口：

| 入口 | 说明 |
| --- | --- |
| http://localhost:8080 | Nginx 统一入口，请求在两个 Web 进程间轮询 |
| http://localhost:8081 | 直连 Web 进程 1（容器 sales-web1） |
| http://localhost:8082 | 直连 Web 进程 2（容器 sales-web2） |

SQLite 数据文件位于命名卷 `sales_data`（容器内 `/data/sales.db`），**容器重启/重建后数据保留**。

打开页面后在「④ 演示脚本与自检」页点**一键准备演示数据**，或：

```bash
curl -X POST http://localhost:8080/api/demo/seed
```

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 上传 ERP 销售 CSV（唯一销售ID、客户ID、交易日期、整数分金额） | `POST /api/sales/import`（multipart）/ `POST /api/sales/import-json`（程序化） |
| 按交易日确定地区，归属入库存储，查询不重算 | `sales.region / region_version / unattributed_reason` 落库，查询接口只读取 |
| 按客户替换整套有效期，左闭右开，末段可无结束日 | `validate_entries`：`[from, to)`，末段 `to=NULL`，背靠背合法、重叠/反向 400 拒绝 |
| 空档标为未归属 | 原因码 `gap`；客户从未建时间线为 `no_timeline` |
| 更正携带所见修订，过期拒绝 | 请求必须带 `expected_version`，与当前版本不符返回 `409 stale_version` |
| 同客户并发更正只允许一份成功 | `correction_mutex` 客户级互斥表 + `BEGIN IMMEDIATE`，跨进程生效，败者 `409 concurrent_correction` |
| 后台回填期间仍可导入、查旧结果，回填不持写锁 | 守护线程做只读计算，sleep 延迟期间零锁；仅批量写 staging 与最终切换拿短锁 |
| 切换纳入期间新增销售 | 切换事务内对 staging 缺失的销售再次查库补算，然后整体重写该客户全部销售 |
| 时间线、修订、全部归属一起生效 | 单个 `BEGIN IMMEDIATE` 事务：旧 timeline 置 dead → 新置 active → 销售重写 → 写 revision，原子提交 |
| 失败保留旧版 | 模拟失败/异常时任务标 failed，删除 backfill 行与 staging，释放互斥，active 旧版原样可用 |
| 每次查询返回同版时间线和销售归属 | 原子提交 + WAL 快照读；`/api/sales/{id}` 附带同版时间线快照，`/api/consistency` 可自检 |
| 两个 Web 进程并发不留下旧归属 | 两容器共享同一 WAL 库；切换是单事务，busy_timeout 30s 串行化写者 |
| 重启另一进程不影响在途回填 | 进程实例表 `process_registry`（实例ID=主机-PID-**随机UUID**）+ 心跳；启动恢复只清理**属主已死**（无注册行/心跳超时）的任务，属主存活的任务、暂存、客户互斥原样保留 |
| 执行进程被强制终止后的恢复 | 属主心跳超时后，任一 Web 进程启动时把遗留任务标 failed、清 backfill/staging、释放 `correction_mutex`；旧 active 时间线、历史销售归属与已生效修订全部保留，客户可立即重新更正 |
| 恢复与切换交错安全 | 回收/失败/切换全部用带 `status IN (...)` 与 `owner_instance_id` 的条件更新；已成功任务不在扫描范围，其他属主的活跃任务不被触碰，僵尸线程醒来无法写回暂存或切换 |
| PID 相同或 PID 被复用 | 归属以含随机 UUID 的 `instance_id` 判定，PID 仅作展示；两个容器 PID 相同、容器重启后 PID 复用都能正确区分 |
| 回填期间新导入销售一起切换 | 切换事务内对 staging 缺失的销售再次查库补算，然后整体重写该客户全部销售 |
| 重启保留 | 命名卷持久化；启动恢复按属主存活状态区分处理（原子提交保证不会出现半切换） |
| 可调回填延迟 | 每次更正请求带 `backfill_delay_ms`（0~300000），延迟均匀分摊到每条销售的计算间隔 |
| 跨生效日样例 | C001 样例销售跨 2026-04-01；追溯模板把生效日提前到 2026-03-01，S1002 回填后改挂华南 |

## 浏览器页面功能

1. **销售导入 / 明细**：上传 CSV、下载样例、按客户/未归属筛选、查看销售详情（含同版时间线快照与未归属原因）。
2. **客户地区有效期**：载入当前时间线（显示所见版本）、可视化增删区间、可调回填延迟、可勾选模拟失败、查看逐版修订 diff。
3. **更正与回填任务**：任务状态（pending/running/succeeded/failed）、暂存进度、每 2 秒自动刷新。
4. **演示脚本与自检**：一键 seed、一致性自检、推荐演示流程。

## 命令行演示

```bash
# 仅依赖标准库；先 seed 再自动演示：并发互斥 → 回填中导入/查询 → 过期版本拒绝
#   → 延迟回填中重启另一进程 → 杀死执行进程后的恢复
python3 demo/concurrency_demo.py http://localhost:8080 http://localhost:8081 http://localhost:8082
```

## 启动恢复的双进程操作示例

回填延迟由请求参数 `backfill_delay_ms` 控制（0~300000 毫秒），延迟均匀分摊到每条销售的
计算间隔，期间不持有任何写锁，导入与查询照常。下面用三个入口区分进程：
`:8080` 为 nginx 轮询入口，`:8081`/`:8082` 分别直连容器 `sales-web1`/`sales-web2`。

### 场景 A：延迟回填进行中，重启另一个 Web 进程

```bash
# 1) 准备演示数据
curl -X POST http://localhost:8080/api/demo/seed

# 2) 固定提交到 web1，给足 20 秒回填窗口
curl -s http://localhost:8081/api/demo/correction-template/C001 \
  | python3 -c 'import sys,json;print(json.dumps(json.load(sys.stdin)["entries"]))'
curl -s -X POST http://localhost:8081/api/corrections -H 'Content-Type: application/json' -d '{
  "customer_id":"C001","expected_version":1,
  "entries":[
    {"region":"华东","effective_from":"2026-01-01","effective_to":"2026-03-01"},
    {"region":"华南","effective_from":"2026-03-01","effective_to":null}],
  "backfill_delay_ms":20000}'

# 3) 回填进行中重启【另一个】进程 web2（web1 的回填不受影响）
docker compose restart web2

# 4) 任务仍是 running，属主存活、暂存与客户占用都在
curl -s http://localhost:8080/api/corrections | python3 -m json.tool | grep -E 'status|owner'

# 5) 回填期间还可以继续导入销售；切换事务会把这些新销售一并切到新版
curl -s -X POST http://localhost:8080/api/sales/import-json -H 'Content-Type: application/json' -d '{
  "rows":[{"sale_id":"S9001","customer_id":"C001","txn_date":"2026-03-15","amount_cents":7700}]}'

# 6) 等待任务完成后，S9001 与历史销售同为新版归属，自检一致
curl -s http://localhost:8080/api/sales/S9001
curl -s http://localhost:8080/api/consistency
```

### 场景 B：执行回填的进程被强制终止，随后恢复

```bash
# 1) 提交一个长延迟回填到 web1
curl -s -X POST http://localhost:8081/api/corrections -H 'Content-Type: application/json' -d '{
  "customer_id":"C002","expected_version":1,
  "entries":[
    {"region":"华北","effective_from":"2026-01-01","effective_to":"2026-05-01"},
    {"region":"西南","effective_from":"2026-06-01","effective_to":null}],
  "backfill_delay_ms":120000}'

# 2) 强制终止执行进程（等价 kill -9，无任何清理机会）
docker kill sales-web1

# 3) 重启任一 Web 进程（重启 web1 或 web2 均可）；启动恢复只回收属主已死的任务
docker compose start web1
# 此时 startup 日志可见：[startup] recovered stale jobs: [<job_id>] by <instance_id>

# 4) 遗留任务为 failed，客户互斥已释放；旧地区归属与已生效修订保留
curl -s http://localhost:8080/api/corrections | python3 -m json.tool | grep -E 'status|error|owner'
curl -s http://localhost:8080/api/timeline/C002     # 仍是旧 active 版本

# 5) 客户可立即重新更正（不再被互斥挡住）
curl -s -X POST http://localhost:8080/api/corrections -H 'Content-Type: application/json' -d '{
  "customer_id":"C002","expected_version":1,
  "entries":[
    {"region":"华北","effective_from":"2026-01-01","effective_to":"2026-05-01"},
    {"region":"西南","effective_from":"2026-06-01","effective_to":null}],
  "backfill_delay_ms":0}'
```

### 场景 C：观察进程实例与 PID 复用区分

```bash
# 查看两个 Web 进程的实例 ID（主机名-PID-随机UUID）与心跳
curl -s http://localhost:8081/api/health
curl -s http://localhost:8082/api/health
curl -s http://localhost:8080/api/admin/workers | python3 -m json.tool

# 即使两个容器内 PID 相同（容器各自有独立 PID 命名空间），UUID 不同即视为不同属主；
# 容器重启后 PID 被复用，新实例 UUID 也不同，旧实例的遗留任务仍会被正确回收。
# 手动触发一次与启动完全相同的恢复（不影响任何属主存活的活跃任务）：
curl -s -X POST http://localhost:8080/api/admin/recover
```

心跳与判定阈值可用环境变量调整（默认心跳 1s、超时 5s）：
`WORKER_HEARTBEAT_MS`、`WORKER_STALE_MS`，也可用 `WORKER_INSTANCE_ID` 显式指定实例 ID。

## 主要 API

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/sales/import` | CSV 上传（表头 `sale_id,customer_id,txn_date,amount_cents`） |
| `POST /api/sales/import-json` | JSON 批量导入 `{"rows":[...]}` |
| `GET /api/sales` / `GET /api/sales/{sale_id}` | 销售列表 / 详情（含同版时间线快照） |
| `GET /api/timeline/{customer_id}` | 当前生效整套时间线 + 所见版本号 |
| `POST /api/timeline/{customer_id}` | 替换有效期（首次建线 expected_version=0） |
| `POST /api/corrections` | 追溯更正（携带 expected_version、delay、fail_simulation） |
| `GET /api/corrections[/{id}]` | 任务列表/详情（含 staging 预览、属主实例与属主存活标记） |
| `GET /api/revisions/{customer_id}` | 修订历史与逐条 diff |
| `GET /api/consistency` | 销售归属与版本时间线一致性自检 |
| `GET /api/admin/workers` | 全部 Web 进程实例：实例ID（含UUID）、PID、心跳年龄、存活状态 |
| `POST /api/admin/recover` | 手动执行与启动时完全相同的恢复逻辑，返回被清理的遗留任务 |
| `POST /api/demo/seed` | 幂等灌入演示数据 |

## 数据模型要点

- `region_timeline`：多版本时间线行，`status ∈ active/backfill/dead`，左闭右开区间。
- `correction_mutex`：`customer_id` 主键，进行中的更正一行锁，切换/失败/回收即释放。
- `correction_job`：更正任务与回填状态（进度、延迟、错误、关联修订、`owner_instance_id` 属主）。
- `region_staging`：回填计算结果，仅在任务存活期间存在，切换后或回收时清理。
- `region_revisions`：每次成功切换一条，含新版本 payload 与新旧逐条 diff；回收不会删除。
- `process_registry`：Web 进程实例注册表，`instance_id=主机-PID-随机UUID`，心跳线程每秒续期；
  启动恢复据此判断任务属主是否存活，死亡实例行保留用于审计与 PID 复用举证。
- `sales`：归属三要素 `region_version / region / unattributed_reason` 持久化。

## 本地测试

```bash
pip install -r requirements.txt
pytest -q
```

覆盖：左闭右开边界、重叠拒绝、空档/no_timeline 未归属、追溯回填、乐观版本过期、
同客户并发互斥、回填期间新增销售纳入切换、失败保留旧版、坏 CSV 整批拒绝、
在途任务对另一进程启动免疫、强杀后遗留任务回收与客户释放、恢复不触碰其他活跃/
已成功任务、僵尸线程守卫、同 PID 不同 UUID 的属主区分，以及基于真实 subprocess
共享 WAL 库的多进程集成测试（含等价 kill -9 的硬终止恢复）。
