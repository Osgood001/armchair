# 运维与恢复

默认状态目录为 `~/.local/share/cryo-jobs`：`config.json`、`jobs.sqlite3`、`jobs/JOB_ID/ATTEMPT/`。每次尝试单独留证，不覆盖前一次日志。停止 daemon 只停止新调度，已启动任务继续；要停任务，使用 `interrupt JOB_ID`。

## 后台常驻

安装后，macOS 可生成并检查 launchd 文件：

```sh
cryo-jobs service launchd --output "$HOME/Library/LaunchAgents/local.cryo-jobs.plist"
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/local.cryo-jobs.plist"
```

Linux 可生成独立的用户服务：

```sh
mkdir -p "$HOME/.config/systemd/user"
cryo-jobs service systemd --output "$HOME/.config/systemd/user/cryo-jobs.service"
systemctl --user daemon-reload
systemctl --user enable --now cryo-jobs.service
```

选择普通 `daemon start` 或系统服务之一，切换前先 `daemon stop`。模板将独立 guardian 留在后台（Linux `KillMode=process`）；停服务不等于停任务。不要把调度器放进主 Cryochamber 服务的进程树后期望 `KillMode=control-group` 停服时它仍存活。服务运行账户须能使用 Agent/飞书/bohr 的认证与 PATH；模板不复制认证信息。Linux 用户服务退出登录后的常驻取决于系统用户管理设置；笔记本睡眠期间不执行，整机重启不等于延续原进程。

macOS/WSL 模板尚未实机运行验证。当前验收为 Linux Python 3.10 的独立守护进程，不宣称 macOS 已验收。

## 状态与恢复操作

```sh
cryo-jobs show JOB_ID
cryo-jobs events --job-id JOB_ID
cryo-jobs interrupt JOB_ID          # 默认最多等 5 秒；--wait 0 只请求
cryo-jobs resume JOB_ID             # 有检查点时显式续跑，同任务新增 attempt
```

`queued → starting → running → succeeded/failed/timed_out/interrupted`。失去 guardian 时进入 `lost`，保留并发名额及资源锁，避免旧任务可能还在执行时启动冲突任务。daemon 崩溃或主会话结束，活着的 guardian 仍执行并落盘；daemon 重启只接管，不重新执行。

整机重启、guardian 被杀或启动窗口崩溃时，最多等待 `launch_grace_seconds` 后判定 `lost`。核对本地进程、远程任务和已产生的副作用，确认已停止后释放保留：

```sh
cryo-jobs resolve JOB_ID --ack-stopped --evidence '已核对进程与远端任务，记录核对结果'
```

没有检查点仍要重试，或核对后决定直接恢复 `lost`，可显式 `resume JOB_ID --ack-uncertain`。这表示操作者承担重复执行判断，不提供恰好一次副作用保证。只有真实检查点适配才能避免从头执行；`resume_command` 可配置单独的续跑 argv。

## 保存和边界

- SQLite 事务保存状态和事件；文件原子替换、fsync 后再确认投递。此版测了进程崩溃，**未做物理断电实验**。
- `interrupt_grace_seconds` / `term_grace_seconds` 默认各 1 秒，然后 SIGKILL；guardian 每 50 ms 检查取消。内核不可中断进程、远端任务或自行脱离进程组的进程不保证停止，不能只凭按钮受理声称全部工作已停止。
- 检查点和结果不包含自动抓取的模型会话；适配器显式保存。共享工作目录只在本调度器内互斥，不能阻止外部进程写入。
- `--request-key` 防止重复注册；新 key 是新任务。取消不自动重试，但调度器无法识别 Agent 用新 key 再次表达同一意图，主 Agent 必须同步删除相关 TODO。
- 可用空间低于 `min_free_bytes`（默认 256 MiB）暂停新调度，当前任务仍继续。日志不自动轮转/删除；定期检查 `doctor` 和日志增长。备份 SQLite 请用 SQLite backup API 或停写后备份，不单独拷贝活跃 WAL 数据库主文件。
- 数据目录按用户隔离，默认新建为 0700；不是多租户安全边界。不要把密码写进 prompt、结果或命令参数。
