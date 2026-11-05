# Router Phase 5：规模化硬化与只读运营遥测

Phase 5 在既有 Router（Phase 0/3/3.5/4）之上补一层**只读运营观测**，
把分散在 `conversation_events`、`budget_ledgers`、`context_pack_manifests`、
任务/运行/审批/回执/派发计划里的状态汇总成一个摘要与一份健康报告。

> 重要门禁：Phase 5 本身**只读**。它不写数据库、不创建任务/回执/运行、
> 不打开开关，也不会进入 enforce。生产默认保持
> `router.worker_dispatch.enabled`、`router.side_effect_writes.enabled`
> 未配置，`router.budget.mode` 未配置或 `shadow`。

## 新增的观测面

| 面 | 形态 | 用途 |
| --- | --- | --- |
| `GET /api/admin/observability/router/summary` | API | 窗口内/累计的 Router 计数、Context Pack token 分位、预算账本合计 |
| `GET /api/admin/observability/router/health` | API | 陈旧租约、未完结回执、预算超限、近期 would-block、生产开关状态 |
| `octop router-obs summary` / `health` | 本地 CLI | 开发/排障时离线读本地 DB（不随蓝绿补丁进生产） |

两个 API 端点均受 `require_permission("observability")` 的既有 ACL 门禁保护。

## `summary` 报告了什么

- **计数**：`conversation_events`（`events`）、任务（`tasks`）、运行（`runs`）、
  审批（`approvals`）、回执（`receipts`）、派发计划（`plans`）、
  Context Pack（`context_packs`）、预算账本（`budget_ledgers`）的
  窗口内/累计两套数值，外加 `main_conversations` 与窗口内 `usage_log` token 合计。
- **Context Pack token**：`count` / `p50` / `p95` / `max` / `sum`，
  用于判断 Context Pack 是否被越做越大（p95 > 6000 视为告警阈值）。
- **预算账本**：活跃账本数、预留/已花费 token 与 cost（micros）、以及
  `over_limit` 行数（`spent_tokens > limit_tokens` 等）。
- **保留策略**：`router.retention.*` 合并默认值后的生效天数。
- **其它**：`schema_version`、`last_event_at`、时间窗口与截止时间。

`window_minutes` 默认 `1440`（一天），API 限定 `1..10080`。

## `health` 报告了什么

任一条件命中即 `status = "warning"`，并给出 `reasons` 列表：

- `task_runs:stale_leases`：`status='running'` 且 `lease_until <= now` 的陈旧租约。
- `write_receipts:processing`：长期停留在 `processing` 的写回执。
- `budget_ledgers:over_limit`：任意预算账本超限。
- `budget:blocked_recent`：近 60 分钟出现 `budget.shadow_checked`(would_block) 或 `budget.blocked`。
- `context_pack:p95_high`：近 60 分钟 Context Pack p95 > 6000。
- 生产开关：`worker:dispatch:enabled`、`side:effect:writes:enabled`、`router:budget:enforce`。

`router.*` settings 解读：

| key | 含义 |
| --- | --- |
| `router.budget.mode` | `shadow`（默认，只观测）/ `enforce`（写拒绝） |
| `router.worker_dispatch.enabled` | 是否开启自动 Worker 派发 |
| `router.side_effect_writes.enabled` | 是否开启写操作审批-回执链路 |
| `router.retention.{event,payload,context_pack,budget_ledger,task_run}_days` | 各类数据保留天数 |

## 使用方式

运维巡检（需 `observability` 权限）：

```bash
# 近一天概览
curl -H "Authorization: Bearer <token>" \
  http://<host>/api/admin/observability/router/summary

# 健康告警与开关状态
curl -H "Authorization: Bearer <token>" \
  http://<host>/api/admin/observability/router/health
```

本地（未部署补丁环境）：

```bash
octop router-obs summary --window-minutes 1440
octop router-obs health
```

## 巡检判读

- `status` 为 `warning` 时应先读 `reasons`，区分「观测到存量风险」与「开关被打开」。
- `budget:blocked_recent` 出现次数应被关注：说明进入 enforce 后会有真实拦截。
- `context_pack:p95_high` 提示上下文在膨胀，需从「最近讨论 + 短期记忆 + 长期知识库」
  的抽取策略上收敛，而不是无限回灌历史（即“信息过载治理”）。
- 生产**不得**把 `shadow` 之外的 `router.budget.mode` 或 worker/side-effect 开关
  随手打开；任何变更都需经 stage -> verify -> promote，并保留回滚路径。

## 与成本、追踪治理的关系

Phase 5 的观测正是长期方案里「Router 层上下文、追踪与成本治理」的执行面：
- **追踪**：全部基于 `trace_id` 贯穿的事件链定位，而不靠把整段会话回灌给模型。
- **预算**：账本 `over_limit` 与 would-block 是可观测的拦截信号，避免隐形 Worker
  无限重试带来的 token 账单失控。
- **保留**：`router.retention.*` 把 Context Pack、事件、账本约束在可回收窗口内，
  配合最近讨论/短期记忆/长期知识库抽取，避免信息量与上下文无限累积。

## 验收与结项（2026-11-05）

前置（Phase 0/3/3.5/4/5）已全部完成，本次完成 Phase 5 生产验收与结项。

**生产状态**

- 蓝绿：`current = 1.0.2-20261105-095728`（active），`previous = 1.0.2-20261104-140650`（回滚路径保留）。
- 服务：`octop` `ActiveState=active`、`NRestarts=0`；本地 `127.0.0.1:8088/api/health` 与公网均 `ok:true`，schema 34。
- 开关：`router.*` settings 为空；Phase 5 只读，未开 `router.side_effect_writes.enabled` /
  `router.worker_dispatch.enabled` / `router.budget.mode=enforce`。
- 新端点：`/api/admin/observability/router/summary`、`/router/health` 已上生产，无 token 返回 401（已挂载且受 ACL 保护）。

**最终验收（只读跑真实 `RouterObservabilityService` 对生产库 `mode=ro` 汇总）**

```
status: healthy（reasons: []）
settings: worker_dispatch_enabled=null, side_effect_writes_enabled=null, budget_mode="shadow"
counts:  events_total=213 / events_window(24h)=36
        context_packs_total=21, main_conversations=2,
        usage_tokens_window(24h)=22,420,248
        tasks/runs/approvals/receipts/plans = 0（dispatch/write 未开，符合设计）
health: stale_running_runs=0, processing_write_receipts=0,
        budget_ledgers_over_limit=0, shadow_would_block_recent=0, budget_blocked_recent=0
```

**验收结论**

- `RouterObservabilityService.health()` 返回 `healthy`、无告警原因。
- real 生产数据为生：212/213 条事件、21 个 Context Pack、近 24h 约 2242 万 usage token，
  预算账本 0 超限、无陈旧租约 / 未完结回执 / 近期 would-block —— 印证 Phase 5 只读观测无副作用。
- 按门禁判为**通过**：Phase 5 完成生产验收，可结项。

**范围说明**

- Phase 6（enforce / 自动 Worker）**不在本次结项范围**，需另行决策后再启动。
- 结项后保留 `previous = 1.0.2-20261104-140650` 作为回滚目标，随时可 `octop-bluegreen rollback`。
