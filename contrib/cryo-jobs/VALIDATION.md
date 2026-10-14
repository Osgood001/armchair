# 验收记录 · 2026-10-14

**21 项进程/状态测试全部通过，安装后的完整命令链路通过。** 环境：Linux 5.15 x86_64、Python 3.10.12；未使用付费模型。

| 验收点 | 实测结果 |
| --- | --- |
| 独立任务并行、最多两个执行、相同资源排队 | 通过 |
| 主调用进程退出、daemon 被 SIGKILL | 工作者继续执行；重启仍为同一次 attempt，无重复效果 |
| 任务运行时重启 daemon | 接管原执行，PID/attempt 保持 |
| 无 daemon 时中断拒绝 SIGINT/TERM 的父子进程 | 0.412 秒确认终止，后续标记未写入，重启不复活 |
| 排队中取消、执行超时、非零退出 | 正确终态，无自动重试 |
| 检查点显式续跑、旧执行写检查点 | 测试适配器从 checkpoint 续跑；拒绝旧 attempt 写入 |
| guardian 丢失 / claim 后尚未 spawn 即崩溃 | `lost`，保留资源与名额，不盲目重放 |
| 飞书操作人校验、回调重复投递、坏事件流 | 拒绝未授权事件；重复回调不误停续跑后的任务 |
| 结果已归档后投递重试 | 不生成重复 inbox 文件 |
| 安装至带空格目录、stdin/字面量参数 | 通过，无第三方 Python 依赖 |
| 安装后执行真实 shell 适配器、结果/inbox/停止守护进程 | 通过，SQLite integrity_check 为 ok |
| 磁盘空间门槛、后端不存在、服务文件 | 空间不足不领新任务；缺失命令正确失败；模板解析/语法验证通过 |

完整套件耗时 **14.453 秒**。证据：[测试日志](evidence/tests-linux.log)、[安装摘要与源码哈希](evidence/install-linux.json)。

复现：`python3 -m unittest discover -s tests -v`；端到端安装：`python3 tests/acceptance.py /absolute/new/evidence-directory`。

未验收：macOS/WSL 实机、真实飞书卡片收发、真实厂商会话恢复、物理断电、远端云任务取消。当前生产 Cryochamber、个人记忆和飞书会话未被修改；测试全部使用独立目录。此处中断耗时是本地进程测试，不能当作飞书端到端延迟承诺。
