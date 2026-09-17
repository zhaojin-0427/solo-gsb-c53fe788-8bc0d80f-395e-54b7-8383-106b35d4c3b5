# 分布式配额决策 API

基于 **Python + FastAPI + PostgreSQL** 的分布式配额决策服务。请求携带
`tenant`、`subject`、`operation`、`cost`、`request_id`，系统在同一事务中原子检查
**三级令牌桶**（租户 → 主体 → 操作），拒绝时返回**所有不足层级**和**最长
`retry_after`**。预占（reserve）立即扣额，提交（commit）不再扣，取消（cancel）或
超时（expired）**只归还一次**。所有一致性保证都由 PostgreSQL 行锁与唯一约束在
数据库层实现，因此应用层可以水平扩展多副本。

## 快速启动

```bash
docker compose up --build
```

启动后：

- API 地址：http://localhost:8000 （交互式文档：http://localhost:8000/docs）
- PostgreSQL：localhost:5432（用户/密码/库名均为 `quota`）

## 配置项（环境变量，均可在 docker-compose.yml 的 app.environment 中调整）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://quota:quota@db:5432/quota` | 数据库连接串 |
| `DEFAULT_CAPACITY` | `1000` | 未配置策略的层级自动采用的桶容量 |
| `DEFAULT_REFILL_RATE` | `100` | 未配置策略的层级自动采用的补充速率（令牌/秒） |
| `RESERVATION_TTL_SECONDS` | `60` | 预占超时时间（秒）；也是单请求 `ttl_seconds` 的上限 |
| `SWEEP_INTERVAL_SECONDS` | `0.5` | 过期预占回收扫描间隔（秒） |
| `SWEEP_BATCH_SIZE` | `100` | 每次扫描回收的最大条数 |

## 三级令牌桶

每个请求同时检查三个层级，键的构成：

| 层级 | 键 |
|---|---|
| `tenant` | (tenant) |
| `subject` | (tenant, subject) |
| `operation` | (tenant, subject, operation) |

桶按 `capacity`（容量）与 `refill_rate`（令牌/秒）惰性补充。层级没有显式策略时
按 `DEFAULT_CAPACITY` / `DEFAULT_REFILL_RATE` 自动创建；可用 `PUT /v1/policies`
为任意层级单独配置。

## API

### `POST /v1/quota/check` — 试算（不扣额）

```bash
curl -X POST http://localhost:8000/v1/quota/check -H 'Content-Type: application/json' -d '{
  "tenant": "acme", "subject": "user-1", "operation": "llm.generate",
  "cost": 5, "request_id": "req-001"
}'
```

### `POST /v1/quota/reserve` — 预占（立即扣额）

请求体同上，可选 `ttl_seconds`（默认并不超过 `RESERVATION_TTL_SECONDS`）。

批准响应：

```json
{
  "request_id": "req-001",
  "decision": "approved",
  "reservation_id": "3f6f…",
  "expires_at": 1758096000.0,
  "ttl_seconds": 60.0
}
```

拒绝响应（HTTP 200，拒绝是一种正常决策结果）：

```json
{
  "request_id": "req-001",
  "decision": "rejected",
  "insufficient": ["tenant", "subject"],
  "retry_after": 2.5
}
```

- `insufficient`：所有不足的层级（按 tenant → subject → operation 顺序）。
- `retry_after`：各不足层级等待时间的**最大值**（秒）；为 `null` 表示在当前策略下
  永远无法满足（`cost` 超过容量或补充速率为 0）。

### `POST /v1/quota/commit` — 提交（不再扣额）

```bash
curl -X POST http://localhost:8000/v1/quota/commit -H 'Content-Type: application/json' \
  -d '{"reservation_id": "3f6f…"}'
```

重复提交返回相同结果（幂等）；对已取消/已过期的预占返回 409。

### `POST /v1/quota/cancel` — 取消（归还一次）

```bash
curl -X POST http://localhost:8000/v1/quota/cancel -H 'Content-Type: application/json' \
  -d '{"reservation_id": "3f6f…"}'
```

重复取消幂等且不会重复归还；对已提交/已过期的预占返回 409。

### 其他接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/v1/reservations/{reservation_id}` | 查询预占状态（active/committed/cancelled/expired） |
| `PUT` | `/v1/policies` | 创建/更新某层级策略：`{"level","tenant","subject"?,"operation"?,"capacity","refill_rate"}` |
| `GET` | `/v1/policies` | 列出全部策略 |
| `GET` | `/v1/buckets?tenant=acme` | 查看桶实时状态（观测用，只读） |
| `GET` | `/healthz` | 健康检查 |

### 幂等与冲突规则

- 同一 `request_id` + 相同参数重试：返回**完全相同**的已存结果（含拒绝结果）。
- 同一 `request_id` + 不同参数（含跨 check/reserve 端点复用）：返回 **409 Conflict**。
- 决策类接口（check/reserve）业务结果一律 HTTP 200；409 仅用于幂等冲突或非法状态流转。

## 一致性设计

- **原子三级检查**：一次决策在一个事务内按固定顺序（tenant → subject → operation）
  对三个桶行加 `SELECT ... FOR UPDATE`，在锁内惰性补充并扣减。固定加锁顺序避免死锁，
  行锁保证并发申请不超卖。
- **幂等**：`requests` 表以 `request_id` 为主键，`INSERT ... ON CONFLICT DO NOTHING`
  会阻塞至并发在途事务提交，然后重放已存响应；响应与扣额在同一事务提交。
- **恰好一次归还**：预占状态机 `active → committed / cancelled / expired` 的跃迁在
  预占行锁内完成，归还只由赢得跃迁的事务在同一事务内执行。取消与后台回收
  （`FOR UPDATE SKIP LOCKED`）竞态时只有一方能跃迁成功，不会重复归还。
- **提交不再扣**：commit 只翻转状态，不触碰令牌。
- **策略更新只影响新申请**：`PUT /v1/policies` 只更新策略行与桶的容量/速率（新决策
  立即生效，桶内余额保留并在下次补充时按新容量截断）；已有预占仍按创建时的 `cost`
  提交/取消/归还，不被改写。
- **多副本安全**：所有不变量都在数据库层强制，应用无状态，可在负载均衡后水平扩展；
  过期回收器在多个副本间通过 `SKIP LOCKED` 瓜分工作。

## 运行测试

```bash
docker compose up --build -d
pip install -r requirements-dev.txt
BASE_URL=http://localhost:8000 pytest tests/ -v
```

测试覆盖：三级拒绝与最长 retry_after、幂等重放与改参冲突、提交/取消幂等、
过期恰好一次归还、取消-提交竞态、并发不超卖、并发同 request_id 结果一致、
策略更新不影响已有预占。

## 目录结构

```
├── docker-compose.yml      # 一键启动（app + postgres）
├── Dockerfile
├── requirements.txt        # 运行时依赖
├── requirements-dev.txt    # 测试依赖
├── app/
│   ├── main.py             # FastAPI 入口、启动建表、过期回收协程
│   ├── service.py          # 核心：三级桶决策、预占状态机、恰好一次归还
│   ├── models.py           # buckets / policies / requests / reservations
│   ├── schemas.py          # 请求体校验
│   ├── config.py           # 环境变量配置
│   └── db.py               # 异步引擎与会话
└── tests/test_api.py       # 集成测试（对运行中的服务发起真实 HTTP 请求）
```
