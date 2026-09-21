# 任务收尾屏障 API（Task Barrier API）

基于 **Python 3.12 + FastAPI + Redis** 的并行任务协调收尾服务：

- 创建屏障时指定**参与者名单**、**达标人数（quorum）**和**截止时间**；
- 参与者以自己的标识**幂等报到**；
- 并发报到中**首次达到达标人数的瞬间，达标成员在 Redis 内被原子冻结**为快照，之后的任何报到（哪怕来自其他名单成员）都不会改写 `quorum` 与 `frozen_at`；
- 支持**主动取消**：取消与达标报到在 Redis 中**原子竞争**，先发生者成为唯一终态（`cancelled` 与 `reached` 互斥）；取消保存 `reason` / `cancelled_at` 并唤醒全部等待者；
- 提供**阻塞等待接口**：达标 / 取消时立即返回终态快照；截止时返回已报到成员与缺席名单；多个等待者通过 Redis Pub/Sub 一并被可靠唤醒（轮询兜底，消息丢失也不会睡死）。

## 一条命令启动（含 Redis）

```bash
docker compose up --build
```

该命令会构建 API 镜像并启动 Redis 7（带健康检查与 AOF 持久化），API 等 Redis 就绪后才启动。停止：`Ctrl+C`（或 `docker compose down`，加 `-v` 同时删除数据卷）。

## 访问地址与端口

| 用途 | 地址 |
| --- | --- |
| API Base URL | <http://localhost:8000> |
| Swagger 交互文档 | <http://localhost:8000/docs> |
| OpenAPI schema | <http://localhost:8000/openapi.json> |
| 健康检查（含 Redis ping） | <http://localhost:8000/health> |

Redis 仅在内网暴露：服务名 `redis`，端口 `6379`，不发布到宿主机。

## 必要配置

全部通过环境变量注入（`docker-compose.yml` 中已给默认值，一般无需修改）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REDIS_URL` | `redis://redis:6379/0` | Redis 连接地址 |
| `BARRIER_TTL_BUFFER_MS` | `86400000`（24h） | 屏障键在截止时间之后额外保留多久，之后由 Redis 自动回收 |
| `BARRIER_POLL_INTERVAL_S` | `0.5` | 等待接口的轮询兜底间隔（秒），Pub/Sub 为主唤醒通道 |
| `BARRIER_MAX_WAIT_S` | `3600` | 单次等待请求最长挂起时间（秒） |

宿主机端口可在 `docker-compose.yml` 的 `api.ports` 修改（如 `"9000:8000"`）。

## API 一览

### 1. 创建屏障 `POST /barriers`

`threshold` 缺省时等于参与者总人数（全员到齐才达标）。`deadline` 为 ISO-8601，不带时区时按 UTC 处理。

```bash
curl -s -X POST http://localhost:8000/barriers \
  -H 'Content-Type: application/json' \
  -d '{"participants":["node-a","node-b","node-c"],"threshold":2,"deadline":"2026-09-20T15:00:00Z"}'
```

返回（`201`，节选）：

```json
{
  "barrier_id": "8f3c…",
  "status": "waiting",
  "participants": ["node-a", "node-b", "node-c"],
  "threshold": 2,
  "deadline": "2026-09-20T15:00:00Z",
  "checked_in": [],
  "checked_count": 0
}
```

校验：`threshold <= 参与者人数`；`deadline` 必须是未来时间；名单会自动去重、去空白。

### 2. 参与者报到 `POST /barriers/{barrier_id}/checkin`

```bash
curl -s -X POST http://localhost:8000/barriers/<id>/checkin \
  -H 'Content-Type: application/json' \
  -d '{"participant_id":"node-a"}'
```

`status` 取值：

- `checked_in`：报到成功，屏障继续等待；
- `already_checked_in`：同一参与者重复报到（幂等，无副作用）；
- `reached`：**本次报到使屏障达标**，`triggered=true`，`quorum` 为冻结成员快照；
- `late`：屏障已达标后才报到，仅记入 `late`，**冻结快照不变**。

非名单成员 → `403`；屏障不存在/已回收 → `404`；截止后或取消后报到 → `409`。

### 3. 主动取消 `POST /barriers/{barrier_id}/cancel`

请求体必须携带**非空** `reason`（纯空白字符串返回 `422`，首尾空白会被去除后保存）：

```bash
curl -s -X POST http://localhost:8000/barriers/<id>/cancel \
  -H 'Content-Type: application/json' \
  -d '{"reason":"上游已放弃等待，提前收尾"}'
```

返回（`200`）：

```json
{
  "barrier_id": "8f3c…",
  "status": "cancelled",
  "reason": "上游已放弃等待，提前收尾",
  "cancelled_at": "2026-09-20T14:58:00.123000Z",
  "view": { "status": "cancelled", "cancel_reason": "…", "cancelled_at": "…", "checked_in": ["node-a"] }
}
```

终态竞争与冲突规则：

- 取消与达标报到由两段 Redis Lua 脚本原子竞争，**先发生者成为唯一终态**：
  - 取消先发生 → 状态 `cancelled`，保存 `reason` 与 `cancelled_at`，此后任何报到返回 `409`（连 `checked_in` 都不会被追加）；
  - 达标先发生 → 取消返回 `409`，`quorum` / `frozen_at` 不变；
  - 已截止 → 取消返回 `409`，缺席名单不变。**任何路径都不得改写既有终态。**
- **重复取消幂等**：返回 `200` 且 `status` 为 `already_cancelled`，`reason` / `cancelled_at` 始终是首次保存的同一份结果（即使本次请求体里的 `reason` 不同）。
- 取消成功会向 Pub/Sub 频道发布 `cancelled`，**唤醒全部已在 `/wait` 挂起的等待者**；发布丢失时仍有轮询兜底。
- 取消时已有的报到名单作为快照保留在 `checked_in` 中；取消本身不是报到行为，不产生报到审计事件。
- 屏障不存在/已回收 → `404`。

### 4. 阻塞等待 `GET /barriers/{barrier_id}/wait`

```bash
curl -s http://localhost:8000/barriers/<id>/wait   # 挂起，直到达标、取消或截止
```

- 达标时立即返回 `status: "reached"`、冻结的 `quorum` 与 `frozen_at`；
- 取消时立即返回 `status: "cancelled"`、`cancel_reason` 与 `cancelled_at`；
- 截止时返回 `status: "expired"`、`checked_in` 已报到名单与 `missing` 缺席名单；
- 任意数量的等待者并发挂起：达标 / 取消各发布一条 Pub/Sub 消息全部唤醒；订阅采用“先订阅后复查 + 轮询兜底”，消息丢失或错过发布也不会漏唤醒（截止不发布消息，由轮询在截止点自然到期兜底）。

### 5. 非阻塞查询 `GET /barriers/{barrier_id}`

随时查看当前 `waiting / reached / expired / cancelled` 状态、报到人数、冻结快照、缺席与迟到名单、取消原因与时间。

### 6. 报到审计事件 `GET /barriers/{barrier_id}/events`

```bash
curl -s 'http://localhost:8000/barriers/<id>/events?cursor=0&limit=100'
```

每次名单成员报到（`checked_in` / `already_checked_in` / `reached` / `late`）都会在与状态变更**同一 Redis 原子操作**中追加一条有序事件；非名单成员、截止后报到等错误请求不产生事件。

返回（节选）：

```json
{
  "barrier_id": "8f3c…",
  "events": [
    {"seq": 1, "participant_id": "node-a", "result": "checked_in", "ts": "2026-09-20T14:59:50.123000Z"},
    {"seq": 2, "participant_id": "node-b", "result": "reached", "ts": "2026-09-20T14:59:51.456000Z"}
  ],
  "next_cursor": 2
}
```

- `cursor`：已读到的最大事件序号（`0` 表示从头开始）；`limit`：单页条数（1–1000，默认 100）；
- 用上一页返回的 `next_cursor` 作为下一页的 `cursor` 即可顺序读完；无新事件时返回空页且 `next_cursor` 不变；
- 事件只增不改、`seq` 连续递增，游标分页在并发报到下稳定、不重不漏；
- 事件与屏障其他数据同生命周期：回收前可查询，回收后一并消失（`404`）。

## 并发与一致性保证

- 报到判定由一段 Lua 脚本完成：`名单校验 → 终态校验（frozen/cancel/截止）→ SADD → 人数判定 → 写入冻结快照` 整体原子执行；取消由另一段 Lua 脚本完成：`终态校验（cancel/frozen/截止）→ 写入取消快照` 整体原子执行。Redis 单线程串行执行脚本，因此达标与取消**必有且仅有一个赢家**：并发报到只有一个请求得到 `reached`，或者取消请求得到 `cancelled`，互斥终态的成员集合、时间点、取消原因均唯一确定。
- 报到审计事件在同一 Lua 脚本内追加（`RPUSH` 到 `events` 列表，序号 = 列表长度 + 1），与状态变更同生共死：要么一起成功，要么都不发生；脚本串行执行保证 `seq` 从 1 连续递增、无空洞无重复。
- 终态落地后脚本的所有写路径都不会触碰对方的终态键：迟到报到只追加到 `checked` 集合（反映在 `late` 字段）；取消后的报到不写任何键；已达标 / 已截止后的取消直接返回冲突。
- 报到响应里的 `PUBLISH reached` 与取消响应里的 `PUBLISH cancelled` 都在脚本执行成功之后进行；即使发布失败，等待者每 `BARRIER_POLL_INTERVAL_S` 秒回读一次真实状态。
- 截止不发布消息（无后台扫描进程），等待者在剩余截止时间上自然到期回读，避免引入额外调度组件。

## 本地不使用容器开发

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
REDIS_URL=redis://localhost:6379/0 uvicorn app.main:app --reload --port 8000
```

需要自行准备一个 Redis 7。

## 项目结构

```
app/
  main.py           FastAPI 路由与错误映射
  barrier_store.py  Redis 键模型 + 原子报到/取消 Lua 脚本（含审计事件）+ Pub/Sub
  service.py        状态机视图组装、阻塞等待
  models.py         请求/响应模型与校验
  config.py         环境变量配置
Dockerfile          API 镜像
docker-compose.yml  api + redis（健康检查、依赖顺序、数据卷）
```
