# Avro 仪器事件格式升级演练平台

面向数仓工程师的 **Avro schema evolution** 演练系统：在升级仪器事件写入 / 读取
格式时，用兼容性守卫保证分析服务始终能够解码，并用持久化的两阶段切换协议处理
丢回执、应用崩溃重启、迟到切换等分布式故障。

技术栈：**FastAPI + SQLite + fastavro**，三个独立进程 / 容器，Docker Compose 一键启动。

## 架构

```
                       ┌─────────────────────────────────────────────┐
                       │  webapp (FastAPI, :8000)                    │
                       │  - 网页 UI + REST API                       │
                       │  - Avro 兼容守卫（avro_compat.py）          │
                       │  - 切换编排 / 持久化 pending / 重启对账     │
                       │  - SQLite: webapp.db（事件、切换、结果）   │
                       └───────────────┬─────────────┬───────────────┘
                                       │ HTTP        │ HTTP
                          ┌────────────▼───┐   ┌─────▼──────────────┐
                          │ analyzer-a     │   │ analyzer-b         │
                          │ 独立读取格式   │   │ 独立读取格式       │
                          │ analyzer-a.db  │   │ analyzer-b.db      │
                          │ (:8001)        │   │ (:8002)            │
                          └────────────────┘   └────────────────────┘
```

两个分析服务**各自独立持久化自己的读取格式**（独立 SQLite 文件 / 独立 Docker
卷），网页服务不持有它们的状态，只通过 `GET /reader/state` 读取其真实生效版本
做对账。

### 兼容性规则（`app/avro_compat.py`，按 Avro 1.11 Schema Resolution 实现）

- 基础类型与完整数值提升链：`int→long→float→double`、`string↔bytes`
- 反向降级（如 `long→int`）拒绝并给出字段路径
- record：字段增删、重排；读取端新增字段**必须有 default**
- array：递归校验元素类型（错误路径带 `[]`）
- union：读取端任一分支可解析即可；写入端 union 的**每个**分支都必须可解析
- 嵌套 record / enum / fixed / map；enum 只允许读取端扩符号
- `aliases` 支持命名类型改名
- 静态检查之外，`roundtrip_check()` 用合成数据做真实二进制编码 / 解码验证

每条拒绝信息都带字段路径，例如：

```
[analyzer-a] $.lab.warehouse.InstrumentEvent.sample_id:
reader 'string' cannot resolve writer 'int' (no numeric/type promotion path)
```

## 切换协议

### 写入格式切换（write format）

1. 已有 pending 切换时拒绝新提议（串行、单调版本号）。
2. 双重守卫：网页本地对 A、B **当前生效读取格式**做静态 + round-trip 校验；
   再调两个分析服务的 `POST /writer/check` 由其各自证明可读。任一不通过即
   **拒绝且不改动当前写入指针**，返回字段路径。
3. 守卫通过后，**先持久化 pending 切换行**，再执行本地写入格式变更
   （带版本栅栏、幂等）。
4. 变更后再次向两个分析服务确认，全部可证实才置 `confirmed`。
5. 丢回执 / 崩溃：切换保持 `pending`，绝不误报成功；后台（每 5 秒）或重启时
   对账，先核实本地指针的真实版本再决定补做还是确认；版本号栅栏保证迟到切换
   永远不能覆盖更新的生效版本（置 `rejected`）。

### 读取格式切换（read format，按服务）

1. 守卫：新读取格式必须能读**当前写入格式**以及**全部留存事件的原始写入格式**
   （逐条历史 schema 校验），失败则返回字段路径且不触碰任何格式。
2. 持久化 pending 后，调分析服务的带栅栏持久化切换接口
   （`switch_id` 幂等、`base_version` 栅栏）。
3. 成功置 `confirmed`；丢回执保持 pending，对账时读取分析服务真实生效版本：
   已生效则凭观测状态确认，未生效则安全重试同一幂等请求，基线过期则拒绝。

### 交错不丢检

事件入仓只读取**已确认的生效写入指针**，pending 期间不阻塞，兼容事件持续入仓；
每条事件留存其写入时的 schema 指纹与版本，读取切换守卫会回溯全部历史事件。

## 快速开始

### Docker Compose

```bash
docker compose up --build
# 网页：     http://localhost:8000
# analyzer-a http://localhost:8001 （独立卷 analyzer_a_data）
# analyzer-b http://localhost:8002 （独立卷 analyzer_b_data）
```

### 本地进程（无 Docker 时）

```bash
pip install -r requirements.txt
ANALYZER_ID=analyzer-a ANALYZER_DB=/data/a.db \
  uvicorn app.analyzer.app:app --port 8001
ANALYZER_ID=analyzer-b ANALYZER_DB=/data/b.db \
  uvicorn app.analyzer.app:app --port 8002
WEBAPP_DB=/data/webapp.db \
ANALYZER_A_URL=http://127.0.0.1:8001 ANALYZER_B_URL=http://127.0.0.1:8002 \
  uvicorn app.webapp.main:app --port 8000
```

## 网页功能

- **① 实际生效格式**：当前写入 schema、A/B 各自读取 schema 与版本、历史写入版本
- **② 切换状态**：pending / confirmed / rejected 全量记录、错误原因、手动对账按钮
- **③ 事件入仓**：平铺表单或完整 Avro record JSON；按当前写入格式 Avro 编码留存；
  可选提交后让任一 / 两个分析服务实际解码
- **④ 格式升级**：写入切换（双服务守卫）、读取切换（含全量历史事件守卫）
- **⑤ 留存事件与解码结果**：事件带写入版本，可随时触发 A/B 解码
- **⑥ 故障注入**：全局延迟、网页侧 / 分析服务侧一次性丢回执（切换、解码）
- **⑦ 项目质控**：项目级非负上下限（带版本号），一键发起全量核查；A、B 按各自
  生效读取格式解码该项目留存事件的**原始 Avro 字节**，按事件号列出缺失 / 非数值 /
  不一致 / 超限原因，其余列为可用；任一服务解码失败整单失败不发布部分结果；
  已发布结论随限值、事件集合、读取版本变化自动显示「待重算」，重启无半份结果

## 项目质控（可信测值交付）

### 数据模型（webapp SQLite）

- `qc_limits`：每个项目一行，`version` 在每次修改限值时单调递增
- `qc_runs`：核查任务，状态 `scanning / published / failed`，并固化发起时的
  限值版本、事件集合 hash、A/B 读取版本、可用 / 问题计数
- `qc_run_events`：**仅在发布事务内写入**，按事件号保存结论、原因与两方数值

### 核查流程与保证

1. 发起时校验项目限值（非负、下限 ≤ 上限），快照完整事件集合（按 payload 的
   `project` 字段筛选）、限值版本与 A/B 生效读取版本，先落 `scanning` 行再后台扫描，
   核查期间入仓、改限值、切读取格式均不阻塞。
2. 逐事件把**原始 Avro 字节**分别发给 A、B，用其当前生效读取格式解码；任一服务
   传输失败或 `decoded_ok=false`，整单置 `failed` 并注明事件号，**绝不发布部分结果**。
3. 逐条判定：任一方测值缺失 / 非数值（布尔、NaN、Infinity、字符串、对象均算非数值）、
   两方有限数值不一致、或任一有限数值超出 `[下限, 上限]`（含边界）即列为问题并给原因，
   否则为可用。
4. 发布前复检基线（限值版本、事件集合 hash、A/B 读取版本），任一漂移即作废失败；
   通过后在**单个 SQLite 事务**内写入全部事件结论并置 `published`。
5. 时效性在读取时动态计算：已发布结论与当前限值版本、完整事件集合、两方读取版本
   全部一致才是「当前有效」，任一不符显示「待重算」及具体原因；`scanning / failed`
   任务永不为当前；分析服务不可达也不能确认结论为当前。
6. 重启时启动恢复把所有遗留 `scanning` 任务置 `failed`（它们从未写过事件结论），
   因此重启后不可能出现半份结果。同一项目同时只允许一个扫描任务。

## REST API 摘要

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/status` | 生效格式、分析服务状态、事件、切换、解码结果（含 `qc` 质控视图） |
| POST | `/api/events` | Avro 编码入仓，`decode_on` 可选触发解码 |
| POST | `/api/events/decode` | 指定分析服务解码留存事件 |
| POST | `/api/switches/writer` | 写入格式切换（守卫 + 两阶段） |
| POST | `/api/switches/reader` | 读取格式切换（全历史守卫 + 栅栏） |
| POST | `/api/reconcile` | 手动核实并继续全部 pending 切换 |
| POST | `/api/simulate` | 延迟 / 丢回执故障注入 |
| GET | `/api/qc/status` | 全部限值与最近核查任务（含 is_current / 待重算原因） |
| POST | `/api/qc/limits` | 设置项目非负上下限（每次修改生成新限值版本） |
| POST | `/api/qc/scan` | 发起项目核查（后台扫描，返回 run_id） |
| GET | `/api/qc/runs/{run_id}` | 单任务详情（published 时含逐事件结论与原因） |

分析服务内部接口：`/health`、`/reader/state`、`/reader/switch`、
`/writer/check`、`/events/decode`、`/simulate`。

## 测试

```bash
# 59 项测试：23 兼容性单元测试 + 13 端到端集成测试 + 8 质控分类单测 + 15 质控端到端测试
# （进程内 ASGI，无需网络；质控测试共 23 项）
PYTHONPATH=. python3 -m unittest discover -s tests -v

# 真实三进程 HTTP 冒烟（先按上面本地方式起好三个服务）
PYTHONPATH=. python3 tests/smoke_live.py
```

## 已验证场景

- 兼容写入切换确认生效；不兼容切换被拦截且写入指针不变，错误带字段路径
- 读取切换同时校验当前写入格式与各版本留存事件，失败不改格式
- 基础类型、数值提升、array、union、字段默认值、嵌套 record、别名
- 旧读取格式解码新事件（投影 + 默认值）；新读取格式解码旧事件
- 丢回执：状态保持 pending、不误报成功；远端已持久化时对账凭观测确认
- 应用重启：启动自动对账，pending 写入切换安全推进或拒绝
- 迟到切换：`base_version` 栅栏拒绝，生效版本不被回退
- 切换与入仓交错：pending 期间兼容事件正常入仓且不丢失
- 幂等重放：同一 `switch_id` 重复调用返回 `already_applied`
- 项目质控：限值校验（非负、有序、NaN/Infinity 拒绝）；全量核查按事件号给出
  可用 / 问题及逐方原因（缺失、非数值、超限、不一致）
- 质控原子性：任一分析服务解码失败（含模拟丢回执）整单失败，无任何部分事件结论
- 质控时效性：入仓、改限值、读取切换后旧结论动态显示待重算，重算后恢复当前；
  扫描中 / 失败任务永不为当前，服务不可达也不能确认当前
- 质控重启恢复：遗留 scanning 任务启动即置 failed，不出现半份结果；扫描期间
  限值 / 事件集合 / 读取版本漂移导致作废
- 质控并发：同一项目同时只允许一个扫描任务

## 限制 / 未覆盖

- 单网页实例（未做网页自身的高可用部署；其状态已全部落 SQLite，重启可恢复）
- 未实现 schema 注册中心的跨命名空间复杂引用（以自包含 record schema 为主）
- 故障注入为一次性开关模型，未模拟持续网络分区
- 质控按 payload 中的 `project` 字段识别项目事件；未单独建立项目维度的事件索引
- fastavro 的数值提升为值保持语义，两个兼容读取格式从同一字节通常解出相等数值；
  「两方不一致」分类由纯函数单测覆盖，端到端侧覆盖缺失 / 非数值 / 超限 / 解码失败
