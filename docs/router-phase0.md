# Router Phase 0：主会话、上下文与预算治理

Phase 0 为移动端主会话建立服务端数据底座，但不替换现有 Thread 聊天链路，也不自动创建 Worker。
当前阶段只观察真实聊天流量，并让 cron / proactive 后台任务经过同一套预算边界。

## 稳定主会话

`main_conversations` 以 `(agent_id, user_id)` 为唯一键，保存稳定的
`conversation_id` 和当前绑定的 `router_thread_id`。迁移会把已有 dashboard Thread
中最新的一个绑定到主会话；旧 Thread API 和历史数据保持不变。

同一次用户输入可以关联：

- `conversation_tasks`：跨多轮仍在进行的任务胶囊。
- `task_runs`：一次有界执行；默认最多 3 次，硬上限 5 次。
- `conversation_events`：按 `trace_id`、任务和运行串起来的审计事件。
- `context_pack_manifests`：本次真正提供给模型的上下文清单。

事件与上下文分离。完整事件用于定位和追踪，但不会整体回灌给模型。

## Context Pack

Phase 0 只组装四类候选：

1. 最近讨论；
2. 短期记忆；
3. 用户可见的长期知识库；
4. 活动任务胶囊。

`infra/conversation/context.py` 对每个 section 和总 token 数设置硬上限，并在
`omitted_refs` / `omitted_by_reason` 中记录未纳入原因。中文、日文、韩文和 emoji
使用保守 token 估算，避免按四字符一 token 低估。

短期记忆只从 Agent 所有者自己的 Memory 读取。共享 Agent 的其他使用者不会看到
Agent 所有者的私有记忆。

## Shadow Router

dashboard 的一轮聊天完成后，`observe_shadow_turn()` 异步记录确定性的路由判断和
Context Pack：

- 不调用额外 LLM；
- 不等待、不阻断当前聊天；
- 不创建任务运行或 Worker；
- 不执行写入、发送、下单等副作用操作。

自动记忆和知识库检索可以通过以下设置关闭：

| 设置键 | 环境变量 | 默认 |
|---|---|---|
| `router.context.memory_enabled` | `OCTOP_ROUTER_CONTEXT_MEMORY_ENABLED` | `true` |
| `router.context.knowledge_enabled` | `OCTOP_ROUTER_CONTEXT_KNOWLEDGE_ENABLED` | `true` |

## 只读 API

所有接口都在 `/api` 下，并校验 Agent 与用户范围：

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/agents/{agent_id}/main-conversation` | 获取稳定主会话 |
| `GET` | `/agents/{agent_id}/main-conversation/events` | 增量读取事件 |
| `GET` | `/agents/{agent_id}/tasks` | 列出任务，可按状态过滤 |
| `GET` | `/agents/{agent_id}/tasks/{task_id}` | 读取任务及运行记录 |
| `GET` | `/agents/{agent_id}/traces/{trace_id}` | 查看一条有界追踪 |

`include_payload=true` 仅管理员可用。

## 预算门

预算默认运行在 `shadow` 模式：记录“本来是否会超限”，但不改变后台任务行为。
`enforce` 默认只作用于 `cron,proactive`，不会阻断 dashboard 交互聊天。

| 设置键 | 环境变量 | 默认 |
|---|---|---|
| `router.budget.mode` | `OCTOP_ROUTER_BUDGET_MODE` | `shadow` |
| `router.budget.enforced_surfaces` | `OCTOP_ROUTER_BUDGET_ENFORCED_SURFACES` | `cron,proactive` |
| `router.budget.user_daily_tokens` | `OCTOP_ROUTER_BUDGET_USER_DAILY_TOKENS` | `400000` |
| `router.budget.agent_daily_tokens` | `OCTOP_ROUTER_BUDGET_AGENT_DAILY_TOKENS` | `600000` |
| `router.budget.global_daily_tokens` | `OCTOP_ROUTER_BUDGET_GLOBAL_DAILY_TOKENS` | `2000000` |
| `router.budget.cost_per_1k_tokens_micros` | `OCTOP_ROUTER_BUDGET_COST_PER_1K_TOKENS_MICROS` | `0` |

单次任务、surface、用户、Agent 和全局预算在同一事务中预留；任一范围不足时全部回滚。
cron 流式执行会在累计 usage 超过预留额时停止；proactive 在调用 LLM 前检查预算，
并在推送完成或失败后结算估算 usage。

## 尚未包含

- 自动 Worker 调度和副作用操作分派；
- Router 直接替代现有 dashboard chat；
- 事件、Context Pack 和 Usage 的归档/删除任务；
- 按模型价格表自动计算费用。

这些属于后续阶段，需要先基于 Phase 0 的真实 trace 做保留周期和成本阈值评估。
