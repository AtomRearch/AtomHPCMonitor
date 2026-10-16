"""Editable date ranges and 24-hour report slots, independent of host timezone."""
import re
import copy
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from .email_html import progress_email, state, SHORT


def report_timezone(settings):
    value = settings.get("timezone", "UTC+08:00")
    match = re.fullmatch(r"UTC([+-])(\d{2}):(\d{2})", value)
    if not match or int(match[2]) > 23 or int(match[3]) > 59:
        raise ValueError("reports.timezone 格式必须为 UTC+08:00 这样的固定偏移")
    minutes = (int(match[2]) * 60 + int(match[3])) * (1 if match[1] == "+" else -1)
    return timezone(timedelta(minutes=minutes))


def validate_settings(settings):
    report_timezone(settings)
    if not isinstance(settings.get("enabled", False), bool):
        raise ValueError("reports.enabled 必须为布尔值")
    if not isinstance(settings.get("only_on_change", True), bool):
        raise ValueError("reports.only_on_change 必须为布尔值")
    if not isinstance(settings.get("times", ["20:00"]), list):
        raise ValueError("reports.times 必须是 HH:MM 字符串数组")
    for value in settings.get("times", ["20:00"]):
        if not isinstance(value, str):
            raise ValueError("reports.times 必须是 HH:MM 字符串数组")
        if not re.fullmatch(r"\d{2}:\d{2}", value):
            raise ValueError("reports.times 使用 24 小时 HH:MM 格式，例如 12:00、20:00")
        if int(value[:2]) > 23 or int(value[3:]) > 59:
            raise ValueError("reports.times 小时为 00–23，分钟为 00–59；午夜使用 00:00")
        time.fromisoformat(value)
    if not settings.get("times", ["20:00"]):
        raise ValueError("reports.times 不能为空")
    if settings.get("start_date") and settings.get("end_date"):
        start, end = date.fromisoformat(settings["start_date"]), date.fromisoformat(settings["end_date"])
        if end < start:
            raise ValueError("reports.end_date 不得早于 start_date")
    elif settings.get("enabled", False):
        raise ValueError("启用定时报告必须填写 start_date 和 end_date")
    window = settings.get("catch_up_minutes", 60)
    if isinstance(window, bool) or not isinstance(window, (int, float)) or not 0 < window < 1440:
        raise ValueError("reports.catch_up_minutes 需 >0 且 <1440")
    if not set(settings.get("programs", ["vasp"])) <= {"g16", "vasp"}:
        raise ValueError("reports.programs 只支持 g16/vasp")


def due_slots(settings, account, now):
    validate_settings(settings)
    if not settings.get("enabled", False) or account not in settings.get("accounts", [account]):
        return []
    local = datetime.fromtimestamp(now, report_timezone(settings))
    slots = []
    # A late-night slot may fall into its catch-up window on the next date.
    for day in (local.date() - timedelta(days=1), local.date()):
        if not date.fromisoformat(settings["start_date"]) <= day <= date.fromisoformat(settings["end_date"]):
            continue
        for slot in sorted(set(settings.get("times", ["20:00"]))):
            scheduled = datetime.combine(day, time.fromisoformat(slot), tzinfo=report_timezone(settings))
            lateness = now - scheduled.timestamp()
            if 0 <= lateness <= settings.get("catch_up_minutes", 60) * 60:
                slots.append({"date": day.isoformat(), "time": slot, "scheduled_at": scheduled.isoformat(),
                              "key": f"report:{account}:{scheduled.isoformat()}"})
    return slots


def report_order(job):
    status = job["status"]
    priority = 2 if status == "SUCCEEDED" else 1 if status in ("RUNNING", "PENDING", "FINALIZING") else 0
    return priority, -(job.get("scheduler", {}).get("end") or job.get("scheduler", {}).get("start") or 0)


def render_report(account, observation, programs, settings, now, slot=None, discovery=None):
    local = datetime.fromtimestamp(now, report_timezone(settings))
    jobs = [job for job in observation["jobs"] if job["program"] in programs]
    jobs.sort(key=report_order)
    incremental = observation.get("report_mode") == "changes"
    label = "进展更新" if incremental else "计算进展"
    counts = dict(Counter(job["status"] for job in jobs))
    subject = f"[AtomHPCMonitor] {label}｜{account}｜{local:%m-%d %H:%M}｜" + (" · ".join(f"{count}项{state(status)}" for status, count in counts.items()) or "暂无登记任务")
    lines = [f"扫描时间: {local.isoformat(timespec='seconds')} ({settings.get('timezone', 'UTC+08:00')})",
             f"账号: {account} | 程序: {', '.join(programs)} | {'本次新增/有进展任务' if incremental else '登记任务'}: {len(jobs)}",
             f"状态汇总: {counts}"]
    if slot:
        lines.append(f"计划时刻: {slot['scheduled_at']}")
    if discovery is not None:
        lines.append(f"自动发现候选 {len(discovery['candidates'])}，本次新增登记 {len(discovery['registered'])}，跳过 {len(discovery['skipped'])}")
        lines.extend(f"发现查询异常: {value}" for value in discovery["errors"])
        lines.extend(f"未自动登记 {item['job_id']} ({item.get('slurm_state', '未知')}): {item['reason']}" for item in discovery["skipped"][:10])
    if not jobs:
        lines.append("本次没有已登记的对应任务；不代表账号没有计算，需核对自动发现结果。")
    lines.append("\n任务进展:")
    for job in jobs[:100]:
        p = job.get("progress", {})
        lines += [f"\n{job['job_id']} | {job['name']} | {job['status']} | {job['task']}",
                  f"目录: {job['workdir']}", f"Slurm: {job['scheduler']['state']}"]
        s = job["scheduler"]
        lines.append(f"PARTITION: {s.get('partition') or '未知'} | NAME: {job['name']} | USER: {s.get('user') or account} | ST: {s.get('short_state') or SHORT.get(s['state'], s['state'])} | TIME: {s.get('elapsed') or '未知'} | NODES: {s.get('nodes') or '未知'} | NODELIST(REASON): {s.get('node_list') or s.get('reason') or '未知'}")
        if job["program"] == "vasp":
            if job["task"] == "sp":
                lines.append("单点自洽计算：不进行离子优化。")
            else:
                lines.append(f"当前已读离子步: {p.get('ionic_step')} / NSW上限 {p.get('ionic_limit')}")
            lines.append(f"当前电子迭代: {p.get('electronic_iteration')} / NELM上限 {p.get('electronic_limit')}")
            lines.append(f"最近已读 TOTEN: {p.get('last_energy_eV')} eV")
        else:
            lines.append(f"Gaussian 当前阶段: {p.get('stage') or '等待日志证据'}；优化步: {p.get('optimization_step')} / {p.get('optimization_limit')}；SCF cycles: {p.get('scf_cycles')}；最近 SCF 能量: {p.get('last_energy_Ha')} Hartree")
            for key, value in p.get("convergence", {}).items():
                lines.append(f"最近收敛表（步 {p.get('convergence_step')}） {key}: {value['value']} / {value['threshold']} {'YES' if value['passed'] else 'NO'}")
            if job["task"] in ("freq", "opt_freq"):
                lines.append(f"已读频率数: {p.get('frequency_count', 0)}；虚频数: {job['program_detail'].get('imaginary_count', '待完整结果')}")
        if job.get("terminal_snapshot") or job.get("historical_import"):
            lines.append(f"{'终态快照' if job.get('terminal_snapshot') else '历史任务回查'}；观察时间: {job['checked_at']}")
        detail = job["program_detail"]
        program_observation = ("运行中，尚未确认程序正常结束"
                               if job["status"] == "RUNNING" and job["program_status"] == "INSUFFICIENT"
                               and job["program_reason"] == "未确认程序正常结束"
                               else f"{job['program_status']}；{job['program_reason']}")
        lines += [f"电子收敛证据: {detail['electronic_converged']} | 几何收敛证据: {detail['geometry_converged']}",
                  f"程序观察: {program_observation}",
                  f"主日志更新: {p.get('log_updated_at')} | 本次读到末尾: {p.get('caught_up', False)}",
                  f"判定: {job['reason']}"]
        lines.extend(f"限制: {issue}" for issue in job["issues"])
        lines.extend(f"调度查询异常: {value}" for value in job["scheduler"].get("query_errors", []))
        lines.extend(f"本次查询异常（保留终态快照）: {value}" for value in job.get("latest_query_errors", []))
    if len(jobs) > 100:
        lines.append(f"其余 {len(jobs) - 100} 项请运行 status 查看；汇总包含全部任务。")
    lines.append("\n离子步/NSW 表示步数与上限，不是优化收敛百分比；未读完大日志时展示的是已读进度。")
    lines.append("仅展示尚未报道的任务或新进展；完整记录使用 status 查看。" if incremental else
                 "汇总包含已登记的运行、等待、完成与异常任务；历史回查不等于完整历史档案。")
    remaining = observation.get("remaining_changes", 0)
    if remaining:
        lines.append(f"另有 {remaining} 项新进展将在后续日报继续报道。")
    lines.append("监控仅查询队列和读取计算文件；邮件 SMTP 接受不等同于手机已收到。")
    payload = {"type": "progress-report", "scanned_at": local.isoformat(), "programs": programs,
               "counts": counts, "jobs": jobs, "slot": slot, "discovery": discovery,
               "report_mode": "changes" if incremental else "full", "remaining_changes": remaining}
    return {"subject": subject, "body": "\n".join(lines), "payload": payload,
            "html_body": progress_email(account, jobs, local.isoformat(timespec='seconds'), counts, slot, discovery,
                                        incremental=incremental, remaining_changes=remaining)}


def progress_snapshot(jobs, programs):
    """Compare calculation facts, never clock ticks, log timestamps or tail text."""
    progress_keys = ("ionic_step", "ionic_limit", "electronic_iteration", "electronic_limit",
                     "last_energy_eV", "stage", "last_energy_Ha", "scf_cycles",
                     "optimization_step", "optimization_limit", "convergence",
                     "convergence_step", "frequency_count")
    scheduler_keys = ("state", "submit", "start", "end", "exit_code", "derived_exit_code", "step_failures")
    detail_keys = ("termination", "electronic_converged", "geometry_converged", "imaginary_count")
    result = {}
    for job in jobs:
        scheduler = job.get("scheduler", {})
        if job.get("program") not in programs or scheduler.get("state") in (None, "UNKNOWN") or scheduler.get("query_errors"):
            continue
        result[job["job_id"]] = {
            "program": job["program"], "task": job.get("task"), "status": job.get("status"),
            "program_status": job.get("program_status"), "program_reason": job.get("program_reason"),
            "scheduler": {key: scheduler.get(key) for key in scheduler_keys},
            "progress": {key: job.get("progress", {}).get(key) for key in progress_keys},
            "detail": {key: job.get("program_detail", {}).get(key) for key in detail_keys}}
    return result


def enqueue_due(store, account, observation, config, now, discovery=None):
    settings = config.get("reports", {})
    ids = []
    for slot in due_slots(settings, account, now):
        if store.report_slot_seen(slot["key"]):
            continue
        programs = settings.get("programs", ["vasp"])
        report_observation = observation
        report_discovery = discovery
        if settings.get("only_on_change", True):
            current = progress_snapshot(observation["jobs"], programs)
            previous = store.report_baseline(account, programs)
            baseline = progress_snapshot(previous["jobs"], programs) if previous else {}
            # Removal/disable and scheduler outages alone are not new progress.
            changed = {job_id for job_id, value in current.items() if job_id not in baseline or value != baseline[job_id]}
            if not changed:
                store.mark_report_slot(account, slot["key"], now, "no-new-progress")
                continue
            jobs = sorted((job for job in observation["jobs"] if job["job_id"] in changed), key=report_order)
            report_observation = {**observation, "jobs": jobs[:100], "report_mode": "changes",
                                  "remaining_changes": max(0, len(jobs) - 100)}
            if discovery is not None:
                report_discovery = copy.deepcopy(discovery)
                known_skips = store.reported_skips(account)
                keys = ("slurm_state", "reason", "workdir")
                report_discovery["skipped"] = [item for item in discovery.get("skipped", [])
                    if item["job_id"] not in known_skips or any(item.get(key) != known_skips[item["job_id"]].get(key) for key in keys)][:10]
        report = render_report(account, report_observation, programs, settings, now, slot, report_discovery)
        message_id = store.enqueue_report(account, slot["key"], report["subject"], report["body"], report["payload"], now, report["html_body"])
        store.mark_report_slot(account, slot["key"], now, "queued")
        if message_id is not None:
            ids.append(message_id)
    return ids
