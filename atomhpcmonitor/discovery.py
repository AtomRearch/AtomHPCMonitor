"""Discover G16/VASP jobs, with guarded recent-history recovery."""
import re
import shlex
import subprocess
import time
from pathlib import Path

from .monitor import initial_runtime
from .slurm import JOB_ID, TERMINAL, FAILURES, timestamp

VASP_EXEC = r"^(?:exec\s+)?(?:(?:\S*/)?(?:srun|mpirun|mpiexec)\s+.*?)?(?:\S*/)?vasp_(?:std|gam|ncl)(?:\s|$)"
G16_EXEC = r"^(?:exec\s+)?(?:(?:\S*/)?(?:srun|mpirun|mpiexec)\s+.*?)?(?:\S*/)?g16(?:\s|$)"


def script_lines(path):
    return [re.split(r"\s+#", ln, maxsplit=1)[0].strip() for ln in read_small(path).splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")]


def output_paths(details, jid):
    paths = []
    for key in ("StdOut", "StdErr"):
        value = details.get(key)
        if value and value not in ("/dev/null", "(null)", "N/A"):
            base, _, index = jid.partition("_")
            value = value.replace("%j", jid).replace("%A", base).replace("%a", index or "4294967294")
            if "%" in value:
                raise ValueError("Slurm 输出路径含未支持的占位符，请人工指定")
            paths.append(value)
    return paths


def gaussian_input(text):
    if "--link1--" in text.lower():
        raise ValueError("多步 Link1 输入需人工核对目标和结束次数")
    routes, reading = [], False
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            reading = True
        if reading:
            if not line.strip():
                break
            routes.append(line.strip())
    route = " ".join(routes).lower()
    if not route:
        raise ValueError("Gaussian 输入缺少可识别 route section")
    if re.search(r"\b(?:irc|scan|admp|bomd|external|td|force|polar|nmr)\b", route):
        raise ValueError("Gaussian 特殊目标暂需人工登记")
    opt, freq = bool(re.search(r"\bopt\b", route)), bool(re.search(r"\bfreq\b", route))
    return "opt_freq" if opt and freq else "opt" if opt else "freq" if freq else "sp"


def gaussian_mapping(code, folder, details, jid):
    commands = [(line, re.search(G16_EXEC, line)) for line in code if re.search(G16_EXEC, line)]
    if len(commands) != 1:
        raise ValueError("Gaussian 执行命令不是单个可识别任务，需人工登记")
    line, match = commands[0]
    lexer = shlex.shlex(line[match.end():], posix=True, punctuation_chars="<>")
    lexer.whitespace_split = True
    tokens = list(lexer)
    if any(value in (">>", "<<") for value in tokens) or re.search(r"\d\s*[<>]", line[match.end():]):
        raise ValueError("Gaussian 复杂重定向需人工确认日志")
    if any(any(x in value for x in ("$", "`", "*", "?", ";", "|", "&")) for value in tokens):
        raise ValueError("Gaussian 命令包含变量/管道等，需人工确认输入输出")
    if "<" in tokens:
        at = tokens.index("<")
        if at + 1 >= len(tokens):
            raise ValueError("Gaussian stdin 路径缺失")
        input_name = tokens[at + 1]
    else:
        input_name = tokens[0] if tokens else ""
    if not input_name or Path(input_name).suffix.lower() not in (".gjf", ".com"):
        raise ValueError("Gaussian 输入路径无法确认")
    input_path = (folder / input_name).resolve()
    input_path.relative_to(folder.resolve())
    task = gaussian_input(read_small(input_path))
    outputs = output_paths(details, jid)
    stdout = output_paths({"StdOut": details.get("StdOut")}, jid)
    if "<" not in tokens and len(tokens) >= 2 and tokens[1] not in ("<", ">"):
        primary = folder / tokens[1]
    elif ">" in tokens:
        at = tokens.index(">")
        if at + 1 >= len(tokens):
            raise ValueError("Gaussian stdout 路径缺失")
        primary = folder / tokens[at + 1]
    elif stdout:
        primary = folder / stdout[0]
    else:
        raise ValueError("Gaussian 未找到可读取的输出日志")
    if str(primary) == "/dev/null":
        raise ValueError("Gaussian 输出被丢弃，需人工指定日志")
    return task, primary, outputs, input_path


def read_small(path):
    with Path(path).open("rb") as handle:
        raw = handle.read(256 * 1024 + 1)
    if len(raw) > 256 * 1024:
        raise ValueError("输入/提交脚本超过 256 KiB，需人工登记")
    return raw.decode("utf-8", errors="replace")


def incar_values(text):
    values = {}
    for line in text.splitlines():
        line = re.split(r"[#!]", line, maxsplit=1)[0]
        for part in line.split(";"):
            match = re.match(r"\s*([A-Za-z][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$", part)
            if match:
                values[match[1].upper()] = match[2].strip()
    return values


def vasp_task(values):
    def scalar(key, default=""):
        # Compatibility for existing inputs such as "100 (Max ionic steps)".
        # Strip only one trailing parenthesized annotation, never arbitrary
        # extra values, decimals or invalid numeric syntax.
        value = str(values.get(key, default)).strip()
        return re.sub(r"[ \t]+\([^()\r\n]*\)[ \t]*$", "", value).strip()
    def number(key, default):
        value = scalar(key, default)
        if not re.fullmatch(r"[+-]?\d+", value):
            raise ValueError(f"INCAR {key} 整数值无法识别，请核对参数与行末注释")
        return int(value)
    if number("IMAGES", 0) > 0 or number("ICHARG", 2) >= 10:
        raise ValueError("NEB/非自洽任务暂不自动登记")
    if any(scalar(key).upper() in (".TRUE.", "T", "TRUE") for key in ("LEPSILON", "LCALCEPS")):
        raise ValueError("响应/介电任务暂不自动登记")
    algo = scalar("ALGO", "NORMAL").upper()
    if algo not in ("NORMAL", "FAST", "VERYFAST", "ALL", "DAMPED", "CONJUGATE", "A", "D", "N", "F", "V"):
        raise ValueError("特殊电子算法暂不自动登记，请核对目标")
    nsw = number("NSW", 0)
    ibrion = number("IBRION", -1 if nsw == 0 else 0)
    if nsw == 0 or ibrion == -1:
        return "sp"
    if ibrion in (1, 2, 3) and nsw > 0:
        return "relax"
    raise ValueError("MD/声子或其他任务暂不自动登记")


def identify(row, details, account, config, spec_builder, programs=("vasp",), historical=False):
    jid = row["job_id"]
    detail_jid = details.get("JobId")
    # Some versions expose an array element's internal numeric id separately.
    if "_" in jid and details.get("ArrayJobId") and details.get("ArrayTaskId"):
        detail_jid = details["ArrayJobId"] + "_" + details["ArrayTaskId"]
    if detail_jid != jid or details.get("UserId", "").split("(", 1)[0] != account:
        raise ValueError("scontrol 身份与队列账号/Job ID 不一致")
    detail_submit = timestamp(details.get("SubmitTime"))
    if row.get("submit") is None or detail_submit != row["submit"]:
        raise ValueError("提交时间缺失或查询期间作业身份发生变化")
    detail_start = timestamp(details.get("StartTime"))
    never_started_failure = historical and row["state"] in FAILURES and row.get("start") is None
    if row["state"] != "PENDING" and not never_started_failure and (row.get("start") is None or detail_start != row["start"]):
        raise ValueError("开始时间缺失或查询期间作业已重新启动")
    folder = Path(details.get("WorkDir", row.get("workdir", "")))
    if not folder.is_absolute() or not folder.is_dir():
        raise ValueError("Slurm 工作目录不可访问")
    script = details.get("Command")
    if not script or not Path(script).is_file():
        raise ValueError("提交脚本不可读取，无法自动确认程序类型")
    code = script_lines(script)
    found = [p for p, pattern in (("vasp", VASP_EXEC), ("g16", G16_EXEC)) if any(re.search(pattern, ln) for ln in code)]
    if len(found) != 1 or found[0] not in programs:
        raise ValueError("提交脚本未找到所选 G16/VASP 的唯一执行命令（不按作业名猜测）")
    program = found[0]
    if program == "vasp" and sum(bool(re.search(VASP_EXEC, line)) for line in code) != 1:
        raise ValueError("提交脚本包含多个 VASP 执行命令，需人工核对目标")
    for line in code:
        if re.search(r"(?:^|[;&]\s*)(?:pushd|popd)\b", line):
            raise ValueError("提交脚本切换目录，需人工指定实际 workdir")
        if re.search(r"(?:^|[;&]\s*)cd\s+", line) and "SLURM_SUBMIT_DIR" not in line:
            raise ValueError("提交脚本可能切换计算目录，需人工指定实际 workdir")
    if program == "vasp":
        task = vasp_task(incar_values(read_small(folder / "INCAR")))
        primary, aux, input_path = folder / "OUTCAR", output_paths(details, jid), folder / "INCAR"
    else:
        task, primary, aux, input_path = gaussian_mapping(code, folder, details, jid)
    if historical:
        if row.get("end") is None or Path(script).stat().st_mtime > row["end"] + 2 or input_path.stat().st_mtime > row["end"] + 2:
            raise ValueError("历史提交脚本/输入在结束后改变，无法确认归属")
    adopt = False
    if primary.exists() and row["state"] != "PENDING" and row.get("start") is not None:
        if primary.stat().st_mtime + 2 < row["start"]:
            raise ValueError("主日志早于本次启动，暂不自动采用旧日志")
        if historical and primary.stat().st_mtime > row["end"] + config.get("settle_seconds", 120) + 2:
            raise ValueError("主日志在历史作业结束后又更新，可能已被后续计算复用")
        adopt = True
    spec = spec_builder({"job_id": jid, "program": program, "task": task,
                         "workdir": str(folder), "name": details.get("JobName") or folder.name,
                         "batch": "auto-" + program, "log": str(primary), "aux_logs": aux,
                         "adopt_existing": adopt}, account, config)
    spec["discovery"] = {"source": "sacct-history" if historical else "squeue+scontrol+submission-script+input",
                         "script": script, "submit": row["submit"], "start": row.get("start")}
    if historical:
        spec["historical_import"] = True
        spec["discovery"]["mapping"] = "directory-fallback" if details.get("HistoryFallback") else "scontrol"
    return spec


def history_details(row):
    """Fallback only when the ended job no longer exists in scontrol."""
    folder = Path(row.get("workdir", ""))
    if not folder.is_absolute() or not folder.is_dir():
        raise ValueError("历史作业 WorkDir 不可访问")
    scripts = []
    for path in folder.iterdir():
        if path.is_file() and path.suffix.lower() in (".slurm", ".sbatch", ".sh"):
            code = script_lines(path)
            if any(re.search(VASP_EXEC, line) or re.search(G16_EXEC, line) for line in code):
                scripts.append(path)
    if len(scripts) != 1:
        raise ValueError("scontrol 已无历史记录，目录内无法唯一确认提交脚本")
    script = scripts[0]
    directives = {}
    for line in read_small(script).splitlines():
        if line.lstrip().startswith("#SBATCH"):
            tokens = shlex.split(line.strip()[7:])
            for option, key in (("-o", "StdOut"), ("--output", "StdOut"), ("-e", "StdErr"), ("--error", "StdErr")):
                for index, token in enumerate(tokens):
                    if token.startswith(option + "="):
                        directives[key] = token.split("=", 1)[1]
                    elif token == option and index + 1 < len(tokens):
                        directives[key] = tokens[index + 1]
    return {"JobId": row["job_id"], "UserId": row["user"], "SubmitTime": row["submit_iso"],
            "StartTime": row["start_iso"], "WorkDir": str(folder), "Command": str(script),
            "JobName": row.get("job_name") or folder.name,
            "HistoryFallback": True,
            "StdOut": directives.get("StdOut", str(folder / "slurm-%j.out")),
            "StdErr": directives.get("StdErr", directives.get("StdOut", str(folder / "slurm-%j.out")))}


def discover(store, account, slurm, config, spec_builder, register=False, program="vasp", history_hours=None, now=None):
    now = time.time() if now is None else now
    programs = config["accounts"][account]["programs"] if program == "all" else [program]
    if not set(programs) <= set(config["accounts"][account]["programs"]):
        raise ValueError(f"账号 {account} 未启用所选程序")
    result = {"account": account, "program": program, "registered": [], "candidates": [], "skipped": [], "errors": []}
    known = {r["spec"]["job_id"] for r in store.jobs(account, enabled_only=False)}
    try:
        rows = slurm.queue(account)
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        result["errors"].append(str(exc)[:500])
        return result
    current_ids = {r["job_id"] for r in rows}
    history = config.get("history", {})
    hours = history_hours if history_hours is not None else history.get("lookback_hours", 24) if history.get("enabled", False) else 0
    if hours:
        try:
            recent = slurm.recent(account, hours)
            recent = [row for row in recent if row["user"] == account and row["state"] in TERMINAL and row.get("end") is not None
                      and now - hours * 3600 <= row["end"] <= now and row["job_id"] not in current_ids and row["job_id"] not in known]
            recent.sort(key=lambda r: (r["end"], r.get("submit") or 0), reverse=True)
            unique = {}
            for row in recent:
                unique.setdefault(row["job_id"], row)
            cap = history.get("max_jobs", 100)
            if len(unique) > cap:
                result["errors"].append(f"历史候选超过 {cap} 项；请缩短回查窗口")
            for row in list(unique.values())[:cap]:
                row["historical"] = True
                rows.append(row)
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
            result["errors"].append("sacct 历史查询: " + str(exc)[:500])
    specs = []
    conflicts = set()
    for row in rows:
        jid = row["job_id"]
        if row["user"] != account or jid in known:
            continue
        if not JOB_ID.fullmatch(jid):
            result["skipped"].append({"job_id": jid, "reason": "不支持聚合数组/异构 Job ID"})
            continue
        try:
            if row.get("historical"):
                if row.get("submit") is None or (row.get("start") is None and row["state"] not in FAILURES):
                    raise ValueError("历史提交/开始时间缺失，无法核对身份")
                from datetime import datetime, timezone
                row["submit_iso"] = datetime.fromtimestamp(row["submit"], timezone.utc).isoformat()
                row["start_iso"] = datetime.fromtimestamp(row["start"], timezone.utc).isoformat() if row.get("start") is not None else "Unknown"
                try:
                    details = slurm.details(jid)
                    if not details.get("Command"):
                        details = history_details(row)
                except (OSError, RuntimeError, subprocess.TimeoutExpired):
                    details = history_details(row)
            else:
                details = slurm.details(jid)
            spec = identify(row, details, account, config, spec_builder, programs, row.get("historical", False))
            duplicates = [previous for previous in specs if previous["log"] == spec["log"]]
            if duplicates:
                conflicts.add(spec["log"])
                for previous in duplicates:
                    specs.remove(previous)
                    result["skipped"].append({"job_id": previous["job_id"], "reason": "多个候选作业共用主日志，无法自动确认归属"})
            if spec["log"] in conflicts:
                raise ValueError("多个候选作业共用主日志，无法自动确认归属")
            store.check_log_mapping([*specs, spec])
            specs.append(spec)
        except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
            result["skipped"].append({"job_id": jid, "workdir": row.get("workdir"), "slurm_state": row["state"], "historical": row.get("historical", False), "reason": str(exc)[:500]})
    result["candidates"] = specs
    if register and specs:
        runtimes = []
        for spec in specs:
            runtime = initial_runtime(spec)
            runtime["submit"] = spec["discovery"]["submit"]
            runtime["run_start"] = spec["discovery"]["start"]
            runtimes.append(runtime)
        store.register_many(specs, runtimes)
        result["registered"] = [spec["job_id"] for spec in specs]
    return result
