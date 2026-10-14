# 接入现有 Agent 与飞书

主 Agent 保留全局记忆；调度器只负责执行账本、进程生命周期和结果事件，不并行改写全局记忆。

## 命令适配契约

`config.json` 的 `command` 是 argv 数组，调用不经 shell。默认在最后追加任务文本，与 Cryochamber 自定义 `run-agent.sh` 契约一致。脚本应前台运行目标 Agent，推荐用 `exec`；不要使用 tmux、nohup、setsid 或后台化绕开生命周期。

| 输入/输出 | 含义 |
| --- | --- |
| `prompt_mode: "argument"` | 默认：任务文本作为最后一个参数 |
| `prompt_mode: "stdin"` / `"file"` | 大上下文可改用标准输入 / 下述文件 |
| `CRYO_JOB_ID` / `CRYO_JOB_ATTEMPT` | 稳定任务身份 / 本次执行编号 |
| `CRYO_JOB_DIR` | 本次日志、制品目录；默认也是 cwd |
| `CRYO_JOB_PROMPT_FILE` | 完整任务文本文件 |
| `CRYO_JOB_CHECKPOINT` | 上一次保存的检查点 JSON；无检查点为 `null` |
| `CRYO_JOB_RESULT` | 可选结果 JSON 写入路径，最大 1 MiB；大制品只记录路径 |

退出码 0 为执行成功，非零为失败；无结果 JSON 也保留完整 stdout/stderr。结果若存在但 JSON 无效，任务标为失败。适配器负责把后端的 session/thread ID 放进检查点，并在续跑时真正恢复它；调度器不会猜测厂商协议或自动续接未保存的模型上下文。

工作者保存检查点：

```sh
cryo-jobs checkpoint "$CRYO_JOB_ID" --json '{"session_id":"backend-session-id","completed_steps":["step-1"],"next":"step-2"}'
```

## 主 Agent 的接入片段

将下面的行为约定加入你自己的主 Agent 指令，再把其 `run-agent.sh` 配到调度器。不要原样复制当前生产 runner 中的用户身份、路由密钥或运维回传行为。

1. 独立长任务使用 `cryo-jobs submit --file task.txt --request-key STABLE_REQUEST_ID` 注册。任务说明包含目标、验收、必要上下文和结果回传要求；主 Agent 不等待执行完毕才回应用户。
2. 修改已有代码仓库时加 `--cwd /absolute/worktree`；相同目录默认串行。不同 worktree 可并行。涉及同一外部资源时，显式使用相同 `--resource KEY`。这属于协作互斥，不是安全沙箱。
3. 全局人物、偏好、项目状态由主 Agent 单点更新；工作者只返回结果、证据与待吸收变化。
4. 终态事件凭 `event_id` 幂等吸收；用户取消后清理对应 TODO，不重新注册同一项工作来绕过取消。
5. `lost` 表示执行效果未知，应先核对原进程与外部动作。不要自动用 `--ack-uncertain` 重放。

初始化时加 `--inbox /your/chamber/messages/inbox`，或显式编辑配置后重启 daemon。系统终态消息使用已有 Cryochamber frontmatter，`from: system`、`source: cryo-jobs`，附任务 ID、事件 ID、结果和证据目录。不会发送飞书消息，也不伪造用户回复义务。已入 inbox/archive 的同名事件不会重新投递；这不保证主 Agent 已完成业务吸收，事件账本仍可查询。

## 飞书中断控制

注册 owner 的真实飞书 `open_id`：初始化加 `--owner OPEN_ID`；`owners` 为空时拒绝所有飞书操作。已有可信桥接进程接收 `card.action.trigger` 后，把事件 JSON 交给 `cryo-jobs control`；原生回调形状和 lark-cli 的扁平事件都可接收。

按钮 `value` 内容为 `{"action":"interrupt","job_id":"JOB_ID"}`。通过本地已认证的 lark-cli 可接连续事件：

```sh
lark-cli event consume card.action.trigger --as bot | cryo-jobs control-stream
```

需在飞书应用后台启用卡片回调；卡片内容读取涉及 `im:message:readonly`。该命令与事件字段已按本机 lark-cli 1.0.86 的 help/schema 核对；本次未连接真实事件流或发送卡片。监听应独立于主 Agent 回合常驻。若已有桥接器，直接调用控制入口，避免重复启动监听。

控制入口只做本地操作人校验和事件去重；**不是公开 HTTP 接口**，不能直接接收未经飞书签名校验的网络请求。`accepted: true` 只表示取消意图落盘，`stopped` 才表示已确认中断；真实终态用 `show` 或终态事件更新交互界面。旧回调重复送达不会中断后来显式续跑的任务。此版本尚不负责卡片发送/更新，也不直接中断现有 Cryochamber 主会话。
