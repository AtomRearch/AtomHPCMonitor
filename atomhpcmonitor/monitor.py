"""Reconcile scheduler evidence and program evidence, then enqueue notifications."""
import copy
import json
import time
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .logs import read_new, snapshot
from .parsers import fresh, feed, program_result
from .slurm import ACTIVE, FAILURES, TERMINAL, nonzero, settled_result, duration

BEIJING = timezone(timedelta(hours=8))


def displayed_time(value):
    return datetime.fromtimestamp(value, BEIJING).isoformat(timespec="seconds") if value is not None else "未知"


def initial_runtime(spec):
    return {"facts": fresh(spec["program"]), "cursor": snapshot(spec["log"], spec["adopt_existing"]),
            "aux": {p: snapshot(p, spec["adopt_existing"]) for p in spec["aux_logs"]},
            "aux_tail": [], "submit": None, "run_start": None, "terminal_since": None,
            "unknown_since": None, "binding_error": None}


def observe(spec, previous, scheduler, now, config):
    runtime = copy.deepcopy(previous)
    issues = []
    state = scheduler["state"]
    if scheduler.get("account_mismatch"):
        issues.append("Slurm 记录账号与登记账号不一致")
    submit, start = scheduler.get("submit"), scheduler.get("start")
    if submit is not None:
        if runtime["submit"] is not None and runtime["submit"] != submit:
            runtime["binding_error"] = "Job ID 对应的提交时间改变，可能是 Job ID 重用；请重新登记"
        else:
            runtime["submit"] = submit
    workdir = scheduler.get("workdir")
    if workdir:
        try:
            Path(spec["workdir"]).resolve().relative_to(Path(workdir).resolve())
        except ValueError:
            issues.append("登记目录不在 Slurm WorkDir 内，请核对作业与日志映射")
    if start is not None:
        if runtime["run_start"] is not None and runtime["run_start"] != start:
            # Automatic requeue may reuse/append logs. Never reuse previous completion evidence.
            runtime["facts"] = fresh(spec["program"])
            runtime["cursor"] = snapshot(spec["log"])
            runtime["aux"] = {p: snapshot(p) for p in spec["aux_logs"]}
            runtime["aux_tail"] = []
            runtime["terminal_since"] = None
            runtime["binding_error"] = "检测到重新启动/重排队；当前日志归属需人工复核后重新登记"
        runtime["run_start"] = start
    if runtime["binding_error"]:
        issues.append(runtime["binding_error"])
    identity_issues = list(issues)

    log_meta = {}
    if state != "PENDING":
        try:
            lines, cursor, log_meta = read_new(spec["log"], runtime["cursor"], config["read_bytes"])
            if log_meta["reset"]:
                runtime["facts"] = fresh(spec["program"])
            for line in lines:
                feed(runtime["facts"], line)
            runtime["cursor"] = cursor
            if start is not None and log_meta["mtime"] + 2 < start:
                issues.append("主日志修改时间早于本次作业开始，疑似旧日志")
            if not log_meta["caught_up"]:
                issues.append("日志增量尚未读完，将在下次巡检继续")
            if not log_meta["stable"]:
                issues.append("本次读取期间日志发生变化，将在下次巡检核验")
            if log_meta["partial_line"]:
                issues.append("日志末尾存在未结束的行")
        except (OSError, ValueError) as exc:
            issues.append(f"主日志不可读取: {type(exc).__name__}: {str(exc)[:300]}")
        for path, cursor in list(runtime["aux"].items()):
            try:
                lines, cursor, meta = read_new(path, cursor, config["read_bytes"])
                runtime["aux"][path] = cursor
                runtime["aux_tail"].extend({"path": path, "text": ln[:500]} for ln in lines[-8:])
                runtime["aux_tail"] = runtime["aux_tail"][-16:]
            except (OSError, ValueError):
                # Optional stderr/stdout evidence is not a substitute for the primary log.
                pass
    program_state, program_reason, detail = program_result(
        runtime["facts"], spec["task"], spec["expected_links"], spec["expected_imaginary"],
        spec["imaginary_tolerance"])
    if runtime["cursor"]["baseline"] and program_state in ("GOAL_NOT_REACHED", "INSUFFICIENT", "NEEDS_REVIEW"):
        program_state = "INSUFFICIENT"
        program_reason = "登记前日志已隔离，部分目标证据可能在未读取区间；" + program_reason
    if start is None and state == "COMPLETED":
        issues.append("缺少 Slurm 实际开始时间，无法排除旧日志")
    if submit is None and state == "COMPLETED":
        issues.append("缺少 Slurm 提交时间，无法确认作业身份")
    if scheduler.get("query_errors") and state == "COMPLETED":
        issues.append("调度查询存在错误，暂不确认计算成功")
    result = {"account": spec["account"], "job_id": spec["job_id"], "name": spec["name"],
              "batch": spec["batch"], "program": spec["program"], "task": spec["task"],
              "workdir": spec["workdir"], "log": spec["log"], "checked_at": displayed_time(now),
              "progress": {"ionic_step": runtime["facts"].get("ionic_index"),
                           "ionic_limit": runtime["facts"].get("nsw"),
                           "electronic_iteration": runtime["facts"].get("electronic_iteration"),
                           "electronic_limit": runtime["facts"].get("nelm"),
                           "last_energy_eV": runtime["facts"].get("last_energy_eV"),
                           "stage": runtime["facts"].get("stage"),
                           "last_energy_Ha": runtime["facts"].get("last_energy_Ha"),
                           "scf_cycles": runtime["facts"].get("scf_cycles"),
                           "optimization_step": runtime["facts"].get("optimization_step"),
                           "optimization_limit": runtime["facts"].get("optimization_limit"),
                           "convergence": runtime["facts"].get("convergence", {}),
                           "convergence_step": runtime["facts"].get("convergence_step"),
                           "frequency_count": len(runtime["facts"].get("frequencies", [])),
                           "log_updated_at": displayed_time(log_meta.get("mtime")),
                           "log_age_seconds": max(0, now - log_meta["mtime"]) if log_meta else None,
                           "caught_up": log_meta.get("caught_up", False)},
              "scheduler": copy.deepcopy(scheduler), "program_status": program_state, "program_reason": program_reason,
              "program_detail": detail, "issues": issues, "aux_tail": runtime["aux_tail"],
              "status": "INSUFFICIENT", "reason": "信息不足", "notify": False}
    seconds_used = scheduler.get("elapsed_seconds")
    if seconds_used is None and start is not None:
        seconds_used = max(0, (scheduler.get("end") if state in TERMINAL and scheduler.get("end") is not None else now) - start)
        result["scheduler"]["elapsed_source"] = "start/end estimate"
    else:
        result["scheduler"]["elapsed_source"] = scheduler.get("source")
    result["scheduler"]["elapsed"] = scheduler.get("elapsed") or duration(seconds_used)
    result["historical_import"] = spec.get("historical_import", False)

    if state in ACTIVE:
        runtime["terminal_since"] = None
        runtime["unknown_since"] = None
        result.update(status="PENDING" if state in {"PENDING", "REQUEUED", "REQUEUE_HOLD", "SPECIAL_EXIT"} else "RUNNING",
                      reason=f"Slurm {state}；程序标记仅作为观察证据")
    elif state in TERMINAL:
        runtime["unknown_since"] = None
        if runtime["terminal_since"] is None:
            runtime["terminal_since"] = now
        elapsed = now - (scheduler.get("end") or runtime["terminal_since"])
        if elapsed < config["settle_seconds"]:
            result.update(status="FINALIZING", reason="等待调度记录和日志落盘")
        else:
            result["notify"] = True
            if identity_issues:
                result.update(status="INSUFFICIENT", reason="；".join(identity_issues))
            elif state in FAILURES:
                result.update(status=state, reason=f"Slurm 明确报告 {state}")
            elif nonzero(scheduler.get("exit_code")) or nonzero(scheduler.get("derived_exit_code")):
                result.update(status="SLURM_FAILED", reason="作业或作业步退出码非零")
            elif scheduler.get("step_failures"):
                result.update(status="SLURM_FAILED", reason="Slurm 作业步存在失败（主作业可能仍显示 COMPLETED）")
            elif scheduler.get("exit_code") != "0:0":
                result.update(status="INSUFFICIENT", reason="Slurm COMPLETED 的退出码缺失或格式异常")
            elif issues:
                result.update(status="INSUFFICIENT", reason="；".join(issues))
                # Continue draining large or changing logs before emitting a provisional unknown.
                if log_meta and (not log_meta["caught_up"] or not log_meta["stable"]):
                    result.update(status="FINALIZING", notify=False)
            else:
                result.update(status=program_state, reason=program_reason)
    else:
        runtime["terminal_since"] = None
        if runtime["unknown_since"] is None:
            runtime["unknown_since"] = now
        result.update(status="INSUFFICIENT", reason="调度状态未知；不根据队列消失或日志缺少结束标记推断失败",
                      notify=now - runtime["unknown_since"] >= config["unknown_seconds"])
    return runtime, result


def batch_summary(rows):
    groups = {}
    for row in rows:
        spec = row["spec"]
        group = groups.setdefault(spec["batch"], {"total": 0, "counts": Counter(), "attention": []})
        status = row["result"]["status"] if row["result"] else "UNOBSERVED"
        group["total"] += 1
        group["counts"][status] += 1
        if status not in ("SUCCEEDED", "PENDING", "RUNNING", "FINALIZING", "UNOBSERVED"):
            group["attention"].append({"job_id": spec["job_id"], "name": spec["name"], "status": status,
                                       "workdir": spec["workdir"]})
    return groups


def notification(spec, runtime, result, summary):
    from .email_html import document, job_card, state, h
    key = json.dumps([spec["account"], spec["job_id"], runtime["submit"],
                      runtime["run_start"], result["status"]], separators=(",", ":"))
    subject = f"[AtomHPCMonitor] {state(result['status'])}｜{spec['account']}｜{spec['name']}｜Job {spec['job_id']}"
    scheduler = result["scheduler"]
    detail = result["program_detail"]
    lines = [result["reason"], f"账号: {spec['account']} | 批次: {spec['batch']}",
             f"程序/目标: {spec['program']} / {spec['task']}", f"目录: {spec['workdir']}",
             f"主日志: {spec['log']}", f"Slurm: {scheduler['state']} | ExitCode: {scheduler.get('exit_code')}",
             f"分区: {scheduler.get('partition') or '未知'} | 时长: {scheduler.get('elapsed') or '未知'} | 节点数: {scheduler.get('nodes') or '未知'} | 节点: {scheduler.get('node_list') or '未知'}",
             f"DerivedExitCode: {scheduler.get('derived_exit_code')}",
             f"开始: {displayed_time(scheduler.get('start'))}", f"结束: {displayed_time(scheduler.get('end'))}",
             f"巡检: {result['checked_at']}（北京时间）",
             f"程序: {result['program_status']} | 结束: {detail['termination']}",
             f"电子收敛证据: {detail['electronic_converged']} | 几何收敛证据: {detail['geometry_converged']}",
             *detail["notes"], "", "关键证据（行号为已解析流的相对行号）:"]
    lines.extend(f"L{item['line']}: {item['text']}" for item in detail["evidence"][-8:])
    if not detail["evidence"]:
        lines.append("无程序关键标记")
    lines.append("\n主日志末尾:")
    lines.extend(item["text"] for item in detail["tail"][-8:])
    if result["aux_tail"]:
        lines.append("\nSlurm stdout/stderr 片段（原始证据，不自动诊断）:")
        lines.extend(f"{x['path']}: {x['text']}" for x in result["aux_tail"][-8:])
    if result["issues"] or scheduler.get("query_errors"):
        lines.append("\n判读限制:")
        lines.extend(result["issues"] + scheduler.get("query_errors", []))
    files = [spec["log"], *spec["aux_logs"]]
    files += ([str(Path(spec["workdir"]) / "1.gjf"), str(Path(spec["workdir"]) / "1.chk")]
              if spec["program"] == "g16" else
              [str(Path(spec["workdir"]) / name) for name in ("INCAR", "OSZICAR", "CONTCAR")])
    lines.append("\n按需下载查看（路径未必均存在）:\n" + "\n".join(files))
    lines.append("\n监控未修改计算文件。SMTP 接受仅表示邮件服务器接受，不等同于手机已收到。")
    body = "\n".join(lines)
    appendix = "\n".join(lines[lines.index("关键证据（行号为已解析流的相对行号）:"):])
    html_body = document(state(result["status"]), f"{spec['account']} · {result['checked_at']}",
                         job_card(result, 1) + f'<div style="font-size:12px;line-height:1.9;white-space:pre-wrap;overflow-wrap:anywhere;">{h(appendix)}</div>',
                         "此通知保存程序与调度证据，未修改计算文件。")
    return {"key": key, "subject": subject, "body": body, "html_body": html_body}


def poll(store, account, slurm, config, now=None, dry_run=False, extra_specs=()):
    now = time.time() if now is None else now
    jobs = store.jobs(account)
    if extra_specs:
        if not dry_run:
            raise ValueError("临时发现任务仅可用于 dry-run")
        known = {row["spec"]["job_id"] for row in jobs}
        jobs.extend({"spec": spec, "runtime": initial_runtime(spec), "result": None}
                    for spec in extra_specs if spec["job_id"] not in known)
    records = slurm.query(account, [row["spec"]["job_id"] for row in jobs])
    for row in jobs:
        current = records[row["spec"]["job_id"]]
        previous = row["result"]
        identity_ok = previous and all(current.get(key) is None or current.get(key) == previous["scheduler"].get(key) for key in ("submit", "start"))
        if settled_result(previous) and identity_ok and not current.get("account_mismatch") and current["state"] in (previous["scheduler"]["state"], "UNKNOWN"):
            row["result"] = copy.deepcopy(previous)
            row["result"].update(notify=False, terminal_snapshot=True)
            row["result"]["latest_query_errors"] = current.get("query_errors", [])
            if current["state"] != "UNKNOWN":
                for key in ("partition", "job_name", "nodes", "node_list", "elapsed", "elapsed_seconds", "short_state"):
                    if current.get(key) is not None:
                        row["result"]["scheduler"][key] = current[key]
                row["result"]["scheduler"]["elapsed_source"] = current.get("source")
        else:
            row["runtime"], row["result"] = observe(row["spec"], row["runtime"], current, now, config)
        if row["spec"].get("historical_import"):
            row["result"]["notify"] = False
    summaries = batch_summary(jobs)
    if not dry_run:
        from .reports import progress_snapshot
        programs = list({row["spec"]["program"] for row in jobs})
        baseline = store.report_baseline(account, programs)
        reported = progress_snapshot(baseline["jobs"], programs) if baseline else {}
        for row in jobs:
            current = progress_snapshot([row["result"]], programs)
            jid = row["spec"]["job_id"]
            if row["result"]["notify"] and jid in current and current[jid] == reported.get(jid):
                row["result"].update(notify=False, notification_suppressed="already-reported")
            event = notification(row["spec"], row["runtime"], row["result"], summaries[row["spec"]["batch"]]) if row["result"]["notify"] else None
            store.save(row["spec"], row["runtime"], row["result"], event, now)
    return {"jobs": [row["result"] for row in jobs], "batches": summaries, "dry_run": dry_run}
