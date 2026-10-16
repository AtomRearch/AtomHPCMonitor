"""Account-specific command line interface; run on the cluster under that account."""
import argparse
import getpass
import json
import os
import shutil
import smtplib
import sqlite3
import ssl
import subprocess
import sys
import time
import uuid
from pathlib import Path

from . import __version__
from .mail import deliver, send, credentials, DeliveryError
from .monitor import initial_runtime, poll, batch_summary, displayed_time
from .slurm import Slurm, JOB_ID
from .store import Store, encode
from .discovery import discover
from .reports import enqueue_due, render_report, validate_settings

TASKS = {"g16": {"sp", "opt", "freq", "opt_freq"}, "vasp": {"sp", "relax"}}


def config_at(path):
    path = Path(path).resolve()
    config = json.loads(path.read_text(encoding="utf-8-sig"))
    config["state_dir"] = str((path.parent / config.get("state_dir", "runtime")).resolve())
    if config.get("smtp", {}).get("credentials_file"):
        config["smtp"]["credentials_file"] = str((path.parent / config["smtp"]["credentials_file"]).resolve())
    defaults = {"poll_seconds": 300, "settle_seconds": 120, "unknown_seconds": 600, "read_bytes": 8388608}
    for key, value in defaults.items():
        config.setdefault(key, value)
        if not isinstance(config[key], (int, float)) or isinstance(config[key], bool) or config[key] < 0:
            raise ValueError(f"{key} 必须为非负数")
    if config["read_bytes"] < 1024 or int(config["read_bytes"]) != config["read_bytes"]:
        raise ValueError("read_bytes 必须为 >=1024 的整数")
    config["read_bytes"] = int(config["read_bytes"])
    if config["poll_seconds"] < 30:
        raise ValueError("poll_seconds 至少 30 秒，避免频繁查询调度器")
    validate_settings(config.get("reports", {}))
    auto = config.get("auto_discovery", {})
    if auto.get("program", "vasp") not in ("g16", "vasp", "all"):
        raise ValueError("auto_discovery.program 支持 g16 / vasp / all")
    history = config.get("history", {})
    if not isinstance(history.get("enabled", False), bool):
        raise ValueError("history.enabled 必须为布尔值")
    for key, default, maximum in (("lookback_hours", 24, 744), ("max_jobs", 100, 1000)):
        value = history.get(key, default)
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
            raise ValueError(f"history.{key} 需为 1–{maximum} 的整数")
    return config


def spec_from(value, account, config):
    value = dict(value)
    jid = str(value["job_id"])
    if not JOB_ID.fullmatch(jid):
        raise ValueError("Job ID 必须为数字或单个数组元素（123_4），不接受整组数组")
    profile = config["accounts"][account]
    program, task = value["program"], value["task"]
    if program not in profile["programs"]:
        raise ValueError(f"账号 {account} 未启用程序 {program}")
    if task not in TASKS[program]:
        raise ValueError(f"{program} 不支持目标 {task}")
    workdir = Path(value["workdir"]).expanduser().resolve()
    if not workdir.is_dir():
        raise ValueError(f"计算目录不存在: {workdir}；请在集群对应账号下登记")
    log = value.get("log") or profile["default_logs"][program].format(job_id=jid)
    log = (workdir / log).resolve()
    aux = value.get("aux_logs")
    if aux is None:
        aux = [f"slurm-{jid}.out", f"slurm-{jid}.err"] if program == "vasp" else []
    aux = [str((workdir / item).resolve()) for item in aux if (workdir / item).resolve() != log]
    links = int(value.get("expected_links", 1))
    imaginary = value.get("expected_imaginary")
    tolerance = float(value.get("imaginary_tolerance", 0))
    if links < 1 or tolerance < 0 or (imaginary is not None and int(imaginary) < 0):
        raise ValueError("Link 数需 >=1，虚频数和阈值需 >=0")
    name = value.get("name") or workdir.name
    batch = value.get("batch") or "default"
    if any("\n" in item or "\r" in item for item in (name, batch, str(workdir), str(log))):
        raise ValueError("任务名、批次或路径不能包含换行")
    adopt = value.get("adopt_existing", False)
    if not isinstance(adopt, bool):
        raise ValueError("adopt_existing 必须是布尔值")
    return {"account": account, "job_id": jid, "program": program, "task": task,
            "name": name, "batch": batch, "workdir": str(workdir), "log": str(log),
            "aux_logs": list(dict.fromkeys(aux)), "adopt_existing": adopt,
            "expected_links": links, "expected_imaginary": int(imaginary) if imaginary is not None else None,
            "imaginary_tolerance": tolerance, "registered_at": time.time()}


def current_user():
    if os.name == "posix":
        import pwd
        return pwd.getpwuid(os.geteuid()).pw_name
    return getpass.getuser()


def require_account(account):
    if current_user() != account:
        raise ValueError(f"实时巡检需在 {account} 的集群登录会话内运行；当前用户为 {current_user()}")


def parser():
    p = argparse.ArgumentParser(description="AtomHPCMonitor — G16/VASP Slurm 只读监控")
    p.add_argument("--version", action="version", version=__version__)
    p.add_argument("--config", required=True, help="明确指定账号配置文件，如 config.local.json")
    p.add_argument("--account", required=True, help="Slurm username (must match the logged-in user)")
    sub = p.add_subparsers(dest="command", required=True)
    add = sub.add_parser("add", help="登记一个作业；默认忽略登记前已有日志内容")
    add.add_argument("--job-id", required=True)
    add.add_argument("--program", choices=list(TASKS), required=True)
    add.add_argument("--task", choices=["sp", "opt", "freq", "opt_freq", "relax"], required=True)
    add.add_argument("--workdir", required=True)
    add.add_argument("--name")
    add.add_argument("--batch", default="default")
    add.add_argument("--log")
    add.add_argument("--aux-log", action="append", dest="aux_logs")
    add.add_argument("--adopt-existing", action="store_true", help="确认现有日志属于该作业，允许从开头读取")
    add.add_argument("--expected-links", type=int, default=1)
    add.add_argument("--expected-imaginary", type=int)
    add.add_argument("--imaginary-tolerance", type=float, default=0)
    imp = sub.add_parser("import-jobs", help="从 JSON 数组批量登记（原子写入）")
    imp.add_argument("manifest")
    disc = sub.add_parser("discover", help="自动发现当前账号的 G16/VASP 作业，默认仅预览")
    disc.add_argument("--program", choices=("g16", "vasp", "all"), default="all")
    disc.add_argument("--history-hours", type=int, help="额外回查最近 N 小时结束的任务，1–744")
    disc.add_argument("--register", action="store_true", help="登记符合身份、脚本和目录检查的候选")
    for command in ("poll", "run"):
        q = sub.add_parser(command, help="巡检一次" if command == "poll" else "持续巡检，可 Ctrl+C 退出")
        q.add_argument("--dry-run", action="store_true", help="不保存本次观察、不入队、不发送；数据库初始化仍可能发生")
        group = q.add_mutually_exclusive_group()
        group.add_argument("--discover", choices=("g16", "vasp", "all"), help="每轮自动发现所选程序")
        group.add_argument("--no-discover", action="store_true", help="本次忽略自动发现配置")
        q.add_argument("--history-hours", type=int, help="额外回查最近 N 小时结束的任务，1–744")
        if command == "run":
            q.add_argument("--json", action="store_true", help="每轮输出完整 JSON；默认只输出简短状态行")
    report = sub.add_parser("report", help="立即扫描并预览进度邮件，--send 才发送这一封")
    report.add_argument("--program", choices=("g16", "vasp", "all"), default="all")
    report.add_argument("--history-hours", type=int, help="额外回查最近 N 小时结束的任务，1–744")
    report.add_argument("--send", action="store_true")
    report.add_argument("--html-output", help="将 HTML 邮件预览保存到指定文件")
    status = sub.add_parser("status", help="已登记任务与批次进度（不访问集群）")
    status.add_argument("--batch")
    sub.add_parser("outbox", help="查看邮件投递状态")
    ev = sub.add_parser("events", help="查看持久化判定证据")
    ev.add_argument("--limit", type=int, default=20)
    disable = sub.add_parser("disable", help="停止某作业巡检，保留记录")
    disable.add_argument("--job-id", required=True)
    rebind = sub.add_parser("rebind", help="人工确认重排队/日志映射后重新建立基线，保留历史事件")
    rebind.add_argument("--job-id", required=True)
    rebind.add_argument("--workdir")
    rebind.add_argument("--log")
    rebind.add_argument("--adopt-existing", action="store_true")
    retry = sub.add_parser("retry-mail", help="手动重试 failed/unknown 邮件")
    retry.add_argument("id", type=int)
    retry.add_argument("--acknowledge-possible-duplicate", action="store_true")
    doctor = sub.add_parser("doctor", help="环境、Slurm 与可选 SMTP_SSL 登录自检；不发邮件")
    doctor.add_argument("--smtp", action="store_true")
    sub.add_parser("test-mail", help="明确发送一封测试邮件（手动执行）")
    sub.add_parser("deliver", help="投递待发邮件，不重新巡检")
    return p


def emit(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, default=dict))


def emit_run_summary(account, result, now, interval):
    from collections import Counter
    discovery = result.get("discovery") or {}
    counts = dict(Counter(job["status"] for job in result["jobs"]))
    mails = dict(Counter(mail["status"] for mail in result.get("mail", [])))
    errors = list(discovery.get("errors", []))
    for job in result["jobs"]:
        errors.extend(job.get("scheduler", {}).get("query_errors", []))
        errors.extend(job.get("latest_query_errors", []))
    message = (f"[{displayed_time(now)}] {account} | 状态 {json.dumps(counts, ensure_ascii=False)}"
               f" | 新登记 {len(discovery.get('registered', []))}"
               f" | 跳过 {len(discovery.get('skipped', []))}"
               f" | 日报入队 {len(result.get('scheduled_reports', []))}"
               f" | 邮件 {json.dumps(mails, ensure_ascii=False)}"
               f" | 查询异常 {len(set(errors))}"
               f" | {interval:g}秒后下次巡检")
    if result.get("dry_run"):
        message += " | dry-run，未保存或发送"
    print(message, flush=True)
    for error in list(dict.fromkeys(errors))[:3]:
        print("查询异常: " + str(error).replace("\n", " ").replace("\r", " "), flush=True)


def doctor(account, config, check_smtp):
    checks = {"python": sys.version.split()[0], "python_ok": sys.version_info >= (3, 7),
              "user": current_user(), "account": account, "account_ok": current_user() == account,
              "sacct": shutil.which("sacct"), "squeue": shutil.which("squeue"), "scontrol": shutil.which("scontrol"),
              "state_dir": config["state_dir"], "smtp_enabled": config["smtp"].get("enabled", False)}
    for tool in ("sacct", "squeue"):
        if checks[tool] and checks["account_ok"]:
            try:
                from .slurm import command
                args = [tool, "--noheader", "--user", account]
                if tool == "sacct":
                    args += ["--starttime", "now-1hours", "--format=JobID,State", "--parsable2"]
                else:
                    args += ["--format=%i|%T"]
                output = command(args)
                checks[tool + "_query"] = {"ok": True, "rows": len(output.splitlines())}
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
                checks[tool + "_query"] = {"ok": False, "error": str(exc)[:500]}
    if check_smtp:
        smtp = config["smtp"]
        values = credentials(smtp)
        sender, password = values["sender"], values["password"]
        if not sender or not password:
            checks["smtp_login"] = {"ok": False, "error": "缺少 SMTP 环境变量或私有配置"}
        else:
            try:
                with smtplib.SMTP_SSL(smtp.get("host", "smtp.qq.com"), int(smtp.get("port", 465)),
                                      timeout=float(smtp.get("timeout", 30)), context=ssl.create_default_context()) as server:
                    server.login(sender, password)
                checks["smtp_login"] = {"ok": True, "note": "SMTP_SSL 登录成功，未发送邮件"}
            except (OSError, smtplib.SMTPException) as exc:
                checks["smtp_login"] = {"ok": False, "error": type(exc).__name__}
    emit(checks)
    need_scontrol = config.get("auto_discovery", {}).get("enabled", False) and account in config.get("auto_discovery", {}).get("accounts", [account])
    return 0 if (checks["python_ok"] and checks["account_ok"] and checks["sacct"] and checks["squeue"] and (checks["scontrol"] or not need_scontrol)
                 and all(v.get("ok", True) for v in checks.values() if isinstance(v, dict))) else 1


def main(argv=None):
    args = parser().parse_args(argv)
    store = None
    try:
        config = config_at(args.config)
        if getattr(args, "history_hours", None) is not None and not 1 <= args.history_hours <= 744:
            raise ValueError("--history-hours 需为1–744的整数")
        account = args.account
        if account not in config["accounts"]:
            raise ValueError(f"配置中没有账号 {account}")
        if args.command == "doctor":
            return doctor(account, config, args.smtp)
        store = Store(Path(config["state_dir"]) / (account + ".sqlite3"))
        if args.command == "status":
            jobs = store.jobs(account, enabled_only=False)
            if args.batch:
                jobs = [r for r in jobs if r["spec"]["batch"] == args.batch]
            emit({"account": account, "jobs": [{"spec": r["spec"], "result": r["result"], "enabled": r["enabled"]} for r in jobs],
                  "batches": batch_summary(jobs)})
            return 0
        if args.command == "outbox":
            emit(store.outbox(account))
            return 0
        if args.command == "events":
            rows = store.db.execute("SELECT * FROM events WHERE account=? ORDER BY id DESC LIMIT ?",
                                    (account, max(1, min(args.limit, 1000))))
            emit([{**dict(row), "payload": json.loads(row["payload"])} for row in rows])
            return 0
        if args.command == "discover":
            require_account(account)
            with store.lock():
                result = discover(store, account, Slurm(), config, spec_from, register=args.register, program=args.program, history_hours=args.history_hours)
            emit(result)
            return 1 if result["errors"] else 0
        if args.command == "report":
            require_account(account)
            with store.lock():
                discovery = None
                extra = []
                discovery = discover(store, account, Slurm(), config, spec_from, register=args.send, program=args.program, history_hours=args.history_hours)
                extra = [] if args.send else discovery["candidates"]
                now = time.time()
                observation = poll(store, account, Slurm(), config, now=now, dry_run=True, extra_specs=extra)
                programs = config["accounts"][account]["programs"] if args.program == "all" else [args.program]
                result = render_report(account, observation, programs, config.get("reports", {}), now, discovery=discovery)
                if args.html_output:
                    output = Path(args.html_output).expanduser().resolve()
                    if output.suffix.lower() not in (".html", ".htm"):
                        raise ValueError("HTML 邮件预览文件需使用 .html 或 .htm 扩展名")
                    output.write_text(result["html_body"], encoding="utf-8")
                    result["html_preview"] = str(output)
                if args.send:
                    key = "manual-report:" + account + ":" + str(uuid.uuid4())
                    mid = store.enqueue_report(account, key, result["subject"], result["body"], result["payload"], now, result["html_body"])
                    result["mail"] = deliver(store, account, {**config["smtp"], "enabled": True}, now, only_ids={mid})
            result.pop("html_body", None)
            emit(result)
            return 1 if args.send and any(row["status"] != "accepted" for row in result.get("mail", [])) else 0
        if args.command in ("poll", "run"):
            require_account(account)
            while True:
                with store.lock():
                    auto = config.get("auto_discovery", {})
                    enabled = args.discover or (auto.get("enabled", False) and account in auto.get("accounts", [account]))
                    discovery = None
                    extra = []
                    if enabled and not args.no_discover:
                        discovery = discover(store, account, Slurm(), config, spec_from, register=not args.dry_run,
                                             program=args.discover or auto.get("program", "all"), history_hours=args.history_hours)
                        extra = discovery["candidates"] if args.dry_run else []
                    now = time.time()
                    result = poll(store, account, Slurm(), config, now=now, dry_run=args.dry_run, extra_specs=extra)
                    result["discovery"] = discovery
                    result["scheduled_reports"] = [] if args.dry_run else enqueue_due(store, account, result, config, now, discovery)
                    result["mail"] = [] if args.dry_run else deliver(store, account, config["smtp"], time.time())
                if args.command == "run" and not args.json:
                    emit_run_summary(account, result, now, config["poll_seconds"])
                else:
                    emit(result)
                if args.command == "poll":
                    return 0
                # Wait outside the lock; Ctrl+C releases cleanly.
                time.sleep(config["poll_seconds"])
        with store.lock():
            if args.command in ("add", "import-jobs"):
                values = [vars(args)] if args.command == "add" else json.loads(Path(args.manifest).read_text(encoding="utf-8-sig"))
                if not isinstance(values, list) or not values:
                    raise ValueError("manifest 需为非空 JSON 数组")
                specs = [spec_from(v, account, config) for v in values]
                ids = [v["job_id"] for v in specs]
                if len(set(ids)) != len(ids):
                    raise ValueError("manifest 内 Job ID 重复")
                runtimes = [initial_runtime(spec) for spec in specs]
                store.register_many(specs, runtimes)
                emit({"registered": specs, "note": "默认忽略登记前已有日志；adopt_existing=true 表示你确认其属于当前作业"})
            elif args.command == "rebind":
                row = next((r for r in store.jobs(account, enabled_only=False) if r["spec"]["job_id"] == args.job_id), None)
                if not row:
                    raise ValueError("该 Job ID 尚未登记")
                value = row["spec"]
                if args.workdir:
                    old_workdir = Path(value["workdir"])
                    try:
                        value["log"] = str(Path(value["log"]).relative_to(old_workdir))
                        value["aux_logs"] = [str(Path(item).relative_to(old_workdir)) for item in value["aux_logs"]]
                    except ValueError:
                        raise ValueError("原日志在计算目录之外，改变目录时请先核对映射") from None
                    value["workdir"] = args.workdir
                if args.log:
                    value["log"] = args.log
                value["adopt_existing"] = args.adopt_existing
                spec = spec_from(value, account, config)
                store.check_log_mapping([spec], exclude_job=args.job_id)
                with store.db:
                    store.db.execute("UPDATE jobs SET spec=?,runtime=?,result=NULL,enabled=1 WHERE account=? AND job_id=?",
                                     (encode(spec), encode(initial_runtime(spec)), account, args.job_id))
                emit({"rebound": spec, "note": "历史事件和邮件保留；已有日志需你确认归属"})
            elif args.command == "disable":
                with store.db:
                    changed = store.db.execute("UPDATE jobs SET enabled=0 WHERE account=? AND job_id=?", (account, args.job_id)).rowcount
                emit({"disabled": bool(changed), "job_id": args.job_id})
            elif args.command == "retry-mail":
                row = next((r for r in store.outbox(account) if r["id"] == args.id), None)
                if not row or row["status"] not in ("failed", "unknown"):
                    raise ValueError("只能重试当前账号的 failed/unknown 邮件")
                if row["status"] == "unknown" and not args.acknowledge_possible_duplicate:
                    raise ValueError("结果不明的投递可能已成功；核验收件后用 --acknowledge-possible-duplicate 重试")
                store.mail_state(row["id"], "pending", time.time())
                emit({"queued": args.id})
            elif args.command == "deliver":
                emit(deliver(store, account, config["smtp"], time.time()))
            elif args.command == "test-mail":
                now = time.time()
                row = {"account": account, "id": "test-" + str(int(now)),
                       "subject": f"[AtomHPCMonitor][TEST] {account}",
                       "body": "AtomHPCMonitor SMTP_SSL 手动测试。请在手机邮箱确认收件。此测试不代表集群巡检已部署。"}
                send(row, config["smtp"])
                emit({"status": "accepted", "note": "SMTP 已接受；请在手机核验收件"})
        return 0
    except KeyboardInterrupt:
        print("监控已退出；巡检与邮件记录已保留。", file=sys.stderr)
        return 130
    except (OSError, ValueError, KeyError, sqlite3.Error, DeliveryError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2
    finally:
        if store:
            store.close()
