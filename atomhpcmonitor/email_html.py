"""Quiet paper-inspired email layouts. Inline tables work without external assets."""
from html import escape
from datetime import datetime

SERIF = "Georgia, 'Palatino Linotype', 'Songti SC', 'SimSun', serif"
INK, MUTED, LINE, PAPER = "#39372f", "#817c70", "#d9d3c5", "#faf7ef"
STATES = {"RUNNING": "运行中", "PENDING": "等待调度", "SUCCEEDED": "计算完成",
          "FINALIZING": "等待日志落盘", "GOAL_NOT_REACHED": "未达到收敛目标",
          "FAILED": "作业失败", "PROGRAM_FAILED": "程序异常", "UNKNOWN": "状态待核验",
          "TIMEOUT": "超过时限", "OUT_OF_MEMORY": "内存不足", "CANCELLED": "已取消",
          "SLURM_FAILED": "作业步失败", "NEEDS_REVIEW": "需要复核", "INSUFFICIENT": "信息不足",
          "NODE_FAIL": "节点故障", "BOOT_FAIL": "节点启动失败", "PREEMPTED": "被抢占"}
EMOJI = {"RUNNING": "⏳", "PENDING": "🕓", "SUCCEEDED": "✅", "FINALIZING": "📝",
         "FAILED": "❌", "PROGRAM_FAILED": "❌", "SLURM_FAILED": "❌", "NODE_FAIL": "❌", "BOOT_FAIL": "❌",
         "TIMEOUT": "⌛", "OUT_OF_MEMORY": "🧠", "CANCELLED": "⏹️", "PREEMPTED": "⏸️",
         "GOAL_NOT_REACHED": "⚠️", "NEEDS_REVIEW": "🔎", "INSUFFICIENT": "❔", "UNKNOWN": "❔"}
SHORT = {"RUNNING": "R", "PENDING": "PD", "COMPLETED": "CD", "FAILED": "F", "TIMEOUT": "TO",
         "OUT_OF_MEMORY": "OOM", "CANCELLED": "CA", "NODE_FAIL": "NF", "BOOT_FAIL": "BF", "PREEMPTED": "PR"}


def h(value):
    return escape(str(value), quote=True)


def state(value):
    return (EMOJI.get(value, "◦") + " " + STATES.get(value, value))


def readable_time(value, full=False):
    if not value:
        return "未记录"
    try:
        stamp = datetime.fromisoformat(value)
        return stamp.strftime("%Y.%m.%d %H:%M" if full else "%m.%d %H:%M:%S")
    except (TypeError, ValueError):
        return str(value)


def document(title, subtitle, content, footer):
    # Texture is optional decoration: clients ignoring gradients keep solid paper.
    return f'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{h(title)}</title></head>
<body style="margin:0;padding:24px 12px;background:#eeeae1;color:{INK};font-family:{SERIF};overflow-wrap:anywhere;word-break:break-word;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr><td align="center">
<table role="presentation" width="640" cellspacing="0" cellpadding="0" style="width:100%;max-width:640px;table-layout:fixed;background-color:{PAPER};background-image:radial-gradient(rgba(99,82,55,.045) .5px,transparent .7px);background-size:5px 5px;border:1px solid {LINE};">
<tr><td style="padding:24px 32px 0;color:{MUTED};font-size:12px;letter-spacing:3px;border-bottom:0;">· · · <span style="float:right">· · ·</span></td></tr>
<tr><td style="padding:26px 32px 34px;">
<div style="font-family:Georgia,serif;font-size:11px;letter-spacing:3px;color:{MUTED};">ATOM RESEARCH &nbsp; / &nbsp; COMPUTATION JOURNAL</div>
<h1 style="font-size:32px;font-weight:400;line-height:1.45;margin:20px 0 10px;letter-spacing:2px;">{h(title)}</h1>
<div style="font-size:13px;line-height:1.9;color:{MUTED};">{h(subtitle)}</div>
<div style="border-top:1px solid {LINE};margin:26px 0 0;height:5px;border-bottom:1px dotted {LINE};"></div>
</td></tr>
<tr><td style="padding:0 32px 12px;">{content}</td></tr>
<tr><td style="padding:24px 32px 28px;border-top:1px dotted {LINE};font-size:12px;color:{MUTED};line-height:1.95;">{h(footer)}
<div style="margin-top:20px;font-family:Georgia,serif;font-size:11px;letter-spacing:2px;">AtomHPCMonitor &nbsp; · &nbsp; · &nbsp; ·</div></td></tr>
</table></td></tr></table></body></html>'''


def metric(label, value):
    return f'<td valign="top" style="padding:12px 8px 12px 0;width:50%;"><div style="font-size:11px;color:{MUTED};line-height:1.8;">{h(label)}</div><div style="font-size:22px;line-height:1.7;">{h(value)}</div></td>'


def scheduler_panel(job):
    s = job["scheduler"]
    node_reason = "(" + str(s.get("reason") or "等待资源") + ")" if s["state"] == "PENDING" else s.get("node_list") or s.get("reason") or "未知"
    fields = [("PARTITION", s.get("partition") or "未知"), ("USER / ST", f"{s.get('user') or job['account']} / {s.get('short_state') or SHORT.get(s['state'], s['state'])}"),
              ("TIME · 作业已运行时长", s.get("elapsed") or "未知"), ("NODES", s.get("nodes") or "未知"), ("NODELIST(REASON)", node_reason)]
    rows = []
    for index in range(0, len(fields), 2):
        cells = []
        for label, value in fields[index:index + 2]:
            cells.append(f'<td valign="top" style="width:50%;padding:8px 8px 8px 0;overflow-wrap:anywhere;"><div style="font-size:10px;color:{MUTED};line-height:1.8;">{h(label)}</div><div style="font-size:13px;line-height:1.9;">{h(value)}</div></td>')
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:8px 0 18px;table-layout:fixed;">' + "".join(rows) + '</table>'


def job_card(job, index):
    p = job.get("progress", {})
    is_sp = job["task"] == "sp"
    kind = {"sp": "单点自洽", "relax": "几何优化", "opt": "几何优化", "freq": "频率分析", "opt_freq": "优化与频率"}.get(job["task"], job["task"])
    detail = job["program_detail"]
    name = job["name"]
    energy = p.get("last_energy_eV")
    energy_text = f"{energy:.8f} eV" if energy is not None else "暂无已读能量"
    metrics = ""
    if job["program"] == "vasp":
        left = ("离子优化（单点计算）", "不适用") if is_sp else ("当前已读离子步 / NSW 上限", f"{p.get('ionic_step')} / {p.get('ionic_limit')}")
        right = ("当前电子迭代 / NELM 上限", f"{p.get('electronic_iteration')} / {p.get('electronic_limit')}")
        metrics = f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>{metric(*left)}{metric(*right)}</tr></table><div style="margin:12px 0 20px;font-size:12px;color:{MUTED};">最近已读 TOTEN <span style="color:{INK};font-size:15px;">{h(energy_text)}</span></div>'
    else:
        left = ("当前阶段", p.get("stage") or "等待日志证据")
        right = ("当前优化步 / 步数上限", f"{p.get('optimization_step') or '未知'} / {p.get('optimization_limit') or '未知'}") if job["task"] in ("opt", "opt_freq") else ("最近已读 SCF cycles", p.get("scf_cycles") if p.get("scf_cycles") is not None else "未知")
        metrics = f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>{metric(*left)}{metric(*right)}</tr></table>'
        if p.get("last_energy_Ha") is not None:
            metrics += f'<div style="font-size:12px;color:{MUTED};margin:12px 0 18px;">最近已读 SCF 能量 <span style="color:{INK};">{h(p["last_energy_Ha"])} Hartree</span></div>'
        if job["task"] in ("opt", "opt_freq") and p.get("scf_cycles") is not None:
            metrics += f'<div style="font-size:12px;color:{MUTED};margin:8px 0 16px;">已读 SCF cycles：{h(p["scf_cycles"])}</div>'
        if p.get("convergence"):
            checks = "<br>".join(f"{h(key)}：{h(value['value'])} / {h(value['threshold'])} · {'✅' if value['passed'] else '○'}" for key, value in p["convergence"].items())
            metrics += f'<div style="font-size:11px;line-height:1.9;color:{MUTED};margin:12px 0 18px;">最近已输出收敛表（步 {h(p.get("convergence_step") or "未知")}）<br>{checks}</div>'
        if job["task"] in ("freq", "opt_freq"):
            metrics += f'<div style="font-size:12px;color:{MUTED};margin:12px 0;">已读频率：{h(p.get("frequency_count", 0))} 项；虚频数：{h(detail.get("imaginary_count", "待完整频率结果"))}</div>'
    evidence = (f"当前电子收敛：{'已有证据' if detail['electronic_converged'] else '尚待证据'}" +
                ("；几何收敛：" + ("已有证据" if detail['geometry_converged'] else "尚待证据") if job["task"] in ("relax", "opt", "opt_freq") else ""))
    limitations = "".join(f'<div style="margin-top:8px;">{h(x)}</div>' for x in job.get("issues", []) + job.get("scheduler", {}).get("query_errors", []) + job.get("latest_query_errors", []))
    reason = "正在计算，尚未确认程序结束。" if job["status"] == "RUNNING" and job["program_reason"] == "未确认程序正常结束" else job["reason"]
    if job["program_status"] == "PROGRAM_FAILED" and job["program_reason"] != reason:
        limitations += f'<div style="margin-top:8px;">程序日志异常证据：{h(job["program_reason"])}</div>'
    return f'''<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-top:1px solid {LINE};margin:0 0 24px;"><tr><td style="padding:22px 0 6px;">
<div style="font-size:11px;letter-spacing:1px;color:{MUTED};">{index:02d} &nbsp; {h(job['program'].upper())} / {h(kind)} &nbsp; · &nbsp; JOB {h(job['job_id'])}</div>
<h2 style="font-size:25px;font-weight:400;margin:12px 0 8px;line-height:1.5;overflow-wrap:anywhere;">{h(name)}</h2>
<div style="font-size:13px;margin-bottom:16px;">{h(state(job['status']))} <span style="color:{MUTED};"> / {h(job['status'])}</span></div>
{scheduler_panel(job)}{metrics}
<div style="font-size:12px;color:{MUTED};line-height:1.9;">{h(evidence)}<br>日志更新：{h(readable_time(p.get('log_updated_at')))}<br>日志读取：{'已读到本轮末尾' if p.get('caught_up') else '仍有内容待追读'}</div>
<div style="margin-top:16px;font-size:13px;line-height:1.95;overflow-wrap:anywhere;word-break:break-word;">{h(reason)}{limitations}</div>
<div style="font-size:11px;color:{MUTED};line-height:1.8;margin-top:8px;">{'已结束，保留终态快照 · ' if job.get('terminal_snapshot') else ''}{'历史任务回查 · ' if job.get('historical_import') else ''}观察时间：{h(readable_time(job['checked_at'], True))}</div>
<div style="margin-top:16px;padding:12px 14px;border-left:1px dotted {LINE};font-size:11px;line-height:1.8;color:{MUTED};overflow-wrap:anywhere;word-break:break-all;">{h(job['workdir'])}</div>
</td></tr></table>'''


def progress_email(account, jobs, local_time, counts, slot=None, discovery=None, incremental=False, remaining_changes=0):
    summary = " &nbsp; · &nbsp; ".join(f"{h(state(key))} <strong style=\"font-weight:400;color:{INK};\">{count}</strong>" for key, count in counts.items()) or "本次没有对应任务"
    label = "本次新增 / 有进展" if incremental else "本次扫描"
    content = f'<div style="font-size:12px;color:{MUTED};letter-spacing:1px;">{label} / {len(jobs)} 项计算</div><div style="font-size:16px;line-height:2;margin:12px 0 28px;">{summary}</div>'
    if incremental:
        content += f'<p style="font-size:12px;color:{MUTED};margin-bottom:20px;">已报道且无变化的任务不再重复列出；完整记录可使用 status 查看。</p>'
    if slot:
        content += f'<div style="font-size:12px;color:{MUTED};margin-bottom:20px;">计划时刻：{h(slot["scheduled_at"])}</div>'
    content += "".join(job_card(job, index) for index, job in enumerate(jobs[:100], 1))
    if len(jobs) > 100:
        content += '<p style="font-size:12px;">其余任务请使用 status 查看，状态汇总已包含全部任务。</p>'
    if remaining_changes:
        content += f'<p style="font-size:12px;">另有 {remaining_changes} 项新进展将在后续日报继续报道。</p>'
    if discovery:
        notices = discovery.get("errors", []) + [f"Job {x['job_id']} / {x.get('slurm_state', '未知')}：{x['reason']}" for x in discovery.get("skipped", [])[:10]]
        if notices:
            content += f'<div style="font-size:12px;line-height:1.9;color:{MUTED};margin:12px 0 24px;">自动发现记录<br>' + "<br>".join(h(x) for x in notices) + '</div>'
    return document("进展更新" if incremental else "计算进展", f"{account} · {readable_time(local_time, True)} · 北京时间" if "+08:00" in local_time else f"{account} · {local_time}", content,
                    "步数与迭代上限用于记录进展，不代表优化收敛百分比。电子收敛针对当前一步，运行中会继续更新。监控仅查询与读取计算文件。")


def text_email(subject, body):
    content = f'<div style="font-size:14px;line-height:2;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word;margin:0 0 32px;">{h(body)}</div>'
    return document("计算通知", subject, content, "原始证据与完整正文同时保留为纯文本，供不同邮件客户端阅读。")
