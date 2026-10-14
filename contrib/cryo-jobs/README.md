# Cryo Jobs 0.1 — Agent 无关的持久任务调度

把长任务从主 Agent 会话中移出：主 Agent 串行理解上下文，独立任务并行执行，终态再写回 Cryochamber inbox。任意命令或 `run-agent.sh` 都可接入，不绑定 Codex、Claude 或模型 SDK。

这是可运行的调度 MVP；当前 Agent 的记忆、TODO、夜间总结继续由现有 Cryochamber 承担。群历史初始化、托管服务和完整个人 Agent 安装包不在这一版。

**已实现：** SQLite 持久任务/事件、默认两个并发名额、共享 workspace 排队、检查点续跑、进程组立即中断、取消不自动复活、调度器重启接管、失联停止重放、飞书控制事件适配。

**运行要求：** Python ≥3.10、POSIX 系统、`ps`，无第三方 Python 依赖。Linux 已实测；macOS 提供 launchd 模板，macOS/WSL 尚未实机验收。数据库放本机磁盘，避免网络共享目录。

## 三步试跑

在本目录运行；把示例命令换成自己的 Agent 启动脚本即可正式接入。

```sh
sh ./install.sh
export PATH="$HOME/.local/bin:$PATH"
# --command-json 是 argv 数组，脚本路径请用绝对路径。
cryo-jobs init --command-json '["/bin/sh", "/absolute/path/examples/run-agent.sh"]'
cryo-jobs doctor
cryo-jobs daemon start
cryo-jobs submit --prompt '完成一项独立任务' --request-key demo-1
cryo-jobs list
cryo-jobs interrupt JOB_ID
```

`CRYO_PYTHON=/path/to/python3 sh ./install.sh` 可选择解释器；`CRYO_INSTALL_PREFIX` 可选择安装位置。`--home /path/to/state` 或 `CRYO_JOBS_HOME` 隔离不同人的任务、配置和日志。安装不会注册系统服务或修改已有 Agent。

飞书与主 Agent 接入见 [INTEGRATION.md](INTEGRATION.md)；后台常驻、检查点、故障处理见 [OPERATIONS.md](OPERATIONS.md)；验收证据见 [VALIDATION.md](VALIDATION.md)。

**边界：** “立即”指直接向本地进程组发中断，不等待模型回合；不服从信号的进程按超时升级终止。远端已提交的云任务、主动脱离进程组的进程，需专门取消适配。`succeeded` 表示命令退出成功，业务是否完成仍由主 Agent 验收。
