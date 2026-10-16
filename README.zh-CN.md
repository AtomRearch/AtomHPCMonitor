# AtomHPCMonitor

**Slurm 集群上的 Gaussian 16 / VASP 只读监控与增量邮件通知。**

[English](README.md) · [简体中文](README.zh-CN.md) · [配置与命令参考](docs/configuration.md) · [MIT 许可证](LICENSE)

![Tests](https://github.com/AtomRearch/AtomHPCMonitor/actions/workflows/tests.yml/badge.svg)

自动发现任务，将 Slurm 状态与程序日志交叉核验，发送完成/异常通知和定时进度邮件。状态与发送记录保存在 SQLite 中，重启后继续跟踪；已经报道且无变化的任务不会在下一封日报里重复出现。

## 功能

- 自动扫描当前账号的 G16/VASP 作业，可按时间窗口回查最近结束的任务。
- 区分调度结束、程序正常终止与登记目标收敛，保留日志判据和限制。
- 日报逐任务比较，只列新增或有进展的任务；完成通知与日报共享已报道记录。
- G16 展示阶段、优化步、SCF cycles/能量、收敛表与频率；VASP 展示离子步、电子迭代与 TOTEN。
- 展示 Job ID、分区、名称、账号、状态、时长、节点及等待原因。
- HTML + 纯文本双正文，淡色纸张质感、衬线字体和充分留白；支持可编辑日期、每日多个时刻与 UTC 偏移。
- 持久化事件与邮件待发队列；明确失败可重试，投递结果不明时保留待核验状态。
- 已确认终态保留快照，避免后续日志复用改写旧结果。

监控仅读取计算输入和输出，不修改、取消、自动修复或重新提交计算。程序独立运行，不依赖 AI API 或聊天窗口。

## 环境要求

- Python **3.7+**，运行时只有标准库依赖。
- Slurm 的 `squeue`、`sacct`、`scontrol`，以及对应计算目录的读取权限。
- 启用邮件时需要支持 `SMTP_SSL` 的邮箱服务器。
- 在集群允许的位置运行，状态目录应可靠支持 SQLite 和文件锁。

实时扫描必须在所选账号自身的登录会话里执行；每个账号独立运行一份监控。CLI 和邮件正文目前为中文。

## 快速开始

```bash
git clone https://github.com/AtomRearch/AtomHPCMonitor.git
cd AtomHPCMonitor
cp config.example.json config.local.json
```

编辑 `config.local.json`：将 `accounts` 的 `hpc_user` 键及 `auto_discovery.accounts`、`reports.accounts` 中的同名占位符改为实际 Slurm 用户名。`gaussian_user` 用于演示第二个仅运行 G16 的账号，可删除或另行配置。

在对应集群账号下执行：

```bash
python3 -m atomhpcmonitor --config config.local.json --account "$(id -un)" doctor
python3 -m atomhpcmonitor --config config.local.json --account "$(id -un)" poll --discover all --history-hours 24 --dry-run
```

`--dry-run` 预览发现及日志观察，不保存任务观察或发送邮件；首次打开状态库仍可能初始化表结构。

### 配置邮件

```bash
cp smtp.example.json smtp.local.json
chmod 600 config.local.json smtp.local.json
```

编辑 `smtp.local.json`，填写发件邮箱、SMTP 授权码和收件邮箱；在 `config.local.json` 中填写正确的 `smtp.host` / `smtp.port`，并按需启用 `smtp.enabled`。示例的 `smtp.example.com` 只是占位符。

```bash
# 只检查SMTP登录，不发邮件。
python3 -m atomhpcmonitor --config config.local.json --account "$(id -un)" doctor --smtp

# 明确发送一封完整报告，核对收件与排版。
python3 -m atomhpcmonitor --config config.local.json --account "$(id -un)" report --program all --send
```

`accepted` 表示 SMTP 服务器接受，不等于已经收到。对既有状态库开启自动邮件前可先查看 `outbox`，此前待发事件也可能投递；`report --send` 只投递明确请求的那一封报告。

### 定时报告与去重

编辑 `reports` 的日期和时刻后启用 `enabled`。起止日期均包含在内，使用24小时 `HH:MM` 格式；示例时刻为 **07:30、13:00、21:30**，`UTC+08:00` 对应北京时间。使用自己的实际日期范围，保留 `only_on_change: true` 即启用增量日报。

新增任务、状态转换、步数/电子迭代、能量、计算阶段或收敛证据变化时才进入日报。无进展的运行任务、已报道的完成/异常结果不重列。扫描时间、运行时长增加或日志时间戳更新不会单独触发报告。完成/异常通知及时发送，并与日报共享逐任务已报道记录。

首次缺少基线且存在可确认任务时发送一份基线报告。自动待发邮件在重试或结果不明期间保留原快照，不用新邮件重复同一结果。每封最多100项变化任务，超出的未报道变化留待后续时刻；手动 `report` 仍提供完整概览。配置修改后重启监控生效。

### 后台运行

在集群允许的监控位置，对每个账号/状态库只运行一份：

```bash
mkdir -p logs
nohup python3 -u -m atomhpcmonitor --config config.local.json --account "$(id -un)" run --discover all --history-hours 24 > "logs/monitor-$(id -un).log" 2>&1 < /dev/null &
echo $! > "logs/monitor-$(id -un).pid"
```

默认300秒一轮，只打印简短摘要；`run --json` 输出完整观察。在原启动节点检查：

```bash
ps -p "$(cat "logs/monitor-$(id -un).pid")" -o pid,etime,args
tail -n 5 "logs/monitor-$(id -un).log"
```

共享 home 不等于跨节点共享进程；`ps` 只能看到相应节点的进程。先确认并停止旧监控，再启动替代进程。升级保留私有配置和 `runtime/`。

## 状态如何理解

| 状态 | 含义 |
|---|---|
| ⏳ `RUNNING` / 🕓 `PENDING` | Slurm 仍报告运行或等待调度。 |
| 📝 `FINALIZING` | 等待最终证据、日志落盘或大日志追读。 |
| ✅ `SUCCEEDED` | 调度、程序终止及登记目标的日志判据通过。 |
| ⚠️ `GOAL_NOT_REACHED` | 已结束，但未确认登记目标收敛。 |
| ❌ `PROGRAM_FAILED` / `SLURM_FAILED` | 程序错误证据或调度/退出码失败。 |
| ⌛ `TIMEOUT` / 🧠 `OUT_OF_MEMORY` / ⏹️ `CANCELLED` | 对应 Slurm 终态。 |
| 🔎 `NEEDS_REVIEW` / ❔ `INSUFFICIENT` | 需要复核或证据不足。 |

任务从队列消失不直接判为成功；VASP 正常结束不代表离子目标收敛。步数和迭代上限不是整体完成百分比，程序判据不替代科学审核。

历史回查依赖有限时间窗口、作业身份、可访问的输入与脚本、日志时间；歧义会跳过。首次导入历史任务只纳入报告，不逐项重发旧完成通知。

## 支持范围与其他命令

G16 目标支持 `sp`、`opt`、`freq`、`opt_freq`；VASP 支持常规自洽单点和几何优化。复杂 Link1 自动发现、变量包装器/复杂重定向，以及 NEB、MD、非自洽 DOS/能带、响应和 GW 等需要进一步适配。支持的 VASP 参数允许尾随单段括号说明。

`discover`、`status`、`events`、`outbox` 用于查看发现、状态、判据和邮件；`add` / `import-jobs` 手动登记；`rebind` 在人工确认重新启动后的日志归属时使用；`retry-mail` 用于明确的手动重试。详见[配置与命令参考](docs/configuration.md)。

## 开发与隐私

```bash
python -m unittest discover -s tests -v
python examples/demo.py --output outputs/demo
```

当前有 **134项测试**。演示使用合成任务，不连接 Slurm/SMTP，核验重启去重与输入文件哈希。[HTML邮件示例](docs/email-preview.html)也使用合成数据。

公开仓库仅含通用账号和合成样本；真实凭据、私有配置、状态库、计算输出和邮件诊断包应保留在本地。诊断导出虽不含 SMTP 凭据，仍含目录和计算片段，公开前需要审阅。参见[安全说明](SECURITY.md)与[贡献指南](CONTRIBUTING.md)。

## 许可证

[MIT](LICENSE)：允许使用、修改及再分发，也允许商业使用；需要保留版权与许可声明。
