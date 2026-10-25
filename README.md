# 麦麦闹钟（local.maibot-clock）

给模型提供自己定闹钟的能力。到点后把**设置时写好的文案**原样发送到创建闹钟的聊天，不唤醒 Planner、不重新生成内容。

## 工具

| 工具 | 作用 |
| --- | --- |
| `clock_set` | 设置闹钟：`kind` = `delay` / `at` / `cron`，`repeat` = `once` / `count` / `forever` |
| `clock_list` | 列出当前聊天的活跃闹钟 |
| `clock_cancel` | 按 `alarm_id` 取消当前聊天的闹钟 |

语义：

- `delay`：`value` 形如 `10m`、`1h30m`、`90`（秒）。配 `count` / `forever` 时按同一间隔重复。
- `at`：`value` 为 ISO-8601，如 `2026-10-26 08:00`；无时区按插件配置时区解释。只能 `once`。
- `cron`：`value` 为标准 5 字段表达式，如 `0 9 * * 1-5`。支持 `*`、`a`、`a-b`、`*/n`、`a-b/n`、逗号列表，周日可写 `0` 或 `7`。
- 闹钟只属于创建它的聊天流，`clock_list` / `clock_cancel` 只能看到本聊天的闹钟。

## 配置

首次加载时由 MaiBot 自动生成 `config.toml`。常用项：

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `timezone` | `Asia/Shanghai` | 解析无时区时间与 cron 的时区 |
| `min_delay_seconds` | `10` | 延时闹钟最小间隔 |
| `max_delay_seconds` | `2592000` | 延时闹钟最大间隔（30 天） |
| `max_alarms_per_stream` | `20` | 单聊闹钟上限 |
| `misfire_grace_seconds` | `300` | 重启后多久内错过的闹钟仍补发 |
| `send_retry_attempts` | `2` | 发送失败重试次数 |
| `send_retry_backoff_seconds` | `30` | 重试间隔 |

## 状态与行为

- 状态保存在插件数据目录的 `alarms.json`（原子写入），重启后恢复。
- 重启时错过触发时间：宽限内补发一次；超过宽限则一次性/固定时间闹钟标记失败，重复闹钟跳到下一个未来时间点，不补发多次。
- 发送失败会重试；仍失败时一次性闹钟标记 `failed`，重复闹钟继续排下一次。
- 不支持跨聊天指定目标、暂停/恢复、自然语言时间解析，也不依赖任何第三方库。
