"""Account-scoped read-only Slurm queries; no shell interpolation."""
import os
import re
import subprocess
from datetime import datetime, timezone

FAILURES = {"FAILED", "TIMEOUT", "OUT_OF_MEMORY", "CANCELLED", "NODE_FAIL",
            "BOOT_FAIL", "DEADLINE", "PREEMPTED", "REVOKED"}
ACTIVE = {"PENDING", "RUNNING", "COMPLETING", "CONFIGURING", "SUSPENDED",
          "RESIZING", "REQUEUED", "REQUEUE_FED", "REQUEUE_HOLD", "SPECIAL_EXIT",
          "STOPPED", "SIGNALING", "STAGE_OUT"}
TERMINAL = FAILURES | {"COMPLETED"}
JOB_ID = re.compile(r"^\d+(?:_\d+)?$")
QUEUE_FORMAT = "%i|%T|%V|%S|%u|%Z|%P|%j|%M|%D|%N|%r|%t"
ACCOUNT_FORMAT = "JobID%64,State%40,ExitCode,DerivedExitCode,Submit,Start,End,User%64,WorkDir%4096,JobName%128,Partition%64,Elapsed,NNodes,NodeList%1024,ElapsedRaw"


def settled_result(result):
    if not result:
        return False
    scheduler = result.get("scheduler", {})
    return (result.get("status") in ((FAILURES - {"PREEMPTED"}) | {"SUCCEEDED", "GOAL_NOT_REACHED", "NEEDS_REVIEW", "PROGRAM_FAILED", "SLURM_FAILED"})
            and scheduler.get("state") in TERMINAL and scheduler.get("submit") is not None
            and (scheduler.get("start") is not None or scheduler.get("state") in FAILURES)
            and scheduler.get("end") is not None and not scheduler.get("query_errors")
            and (result.get("progress", {}).get("caught_up") or any(x.startswith("主日志不可读取") for x in result.get("issues", []))))


def seconds(value):
    try:
        days, _, clock = str(value).rpartition("-")
        parts = [int(x) for x in clock.split(":")]
        if len(parts) not in (2, 3):
            return None
        return (int(days or 0) * 86400 + (parts[0] * 3600 + parts[1] * 60 + parts[2] if len(parts) == 3 else parts[0] * 60 + parts[1]))
    except ValueError:
        return None


def duration(value):
    if value is None:
        return "未知"
    total = max(0, int(value))
    days, total = divmod(total, 86400)
    hours, total = divmod(total, 3600)
    minutes, secs = divmod(total, 60)
    return (str(days) + "-" if days else "") + f"{hours:02d}:{minutes:02d}:{secs:02d}"


def timestamp(value):
    if not value or value in ("Unknown", "None", "N/A", "Invalid"):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=timezone.utc).timestamp() if parsed.tzinfo is None else parsed.timestamp()
    except ValueError:
        return None


def normalize(state):
    return state.split()[0].rstrip("+") if state.strip() else "UNKNOWN"


def command(args, timeout=30):
    env = dict(os.environ, TZ="UTC", SLURM_TIME_FORMAT="standard", LC_ALL="C")
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                            env=env, check=False)
    if result.returncode:
        raise RuntimeError(f"{args[0]} 查询失败 (exit {result.returncode}): {result.stderr[:500].strip()}")
    return result.stdout


def parse_sacct(text):
    rows = []
    for line in text.splitlines():
        fields = line.split("|")
        if len(fields) in (10, 16) and fields[-1] == "":
            fields.pop()
        # WorkDir is the last field and can be empty.
        if len(fields) == 8:
            fields.append("")
        if len(fields) not in (9, 15):
            continue
        jid, state, exit_code, derived, submit, start, end, user, workdir = [x.strip() for x in fields[:9]]
        rows.append({"job_id": jid, "state": normalize(state), "exit_code": exit_code,
                     "derived_exit_code": derived, "submit": timestamp(submit),
                     "start": timestamp(start), "end": timestamp(end),
                     "user": user, "workdir": workdir, "raw": line[:1200], "source": "sacct"})
        if len(fields) == 15:
            extras = dict(zip(("job_name", "partition", "elapsed", "nodes", "node_list", "elapsed_raw"), (x.strip() for x in fields[9:])))
            extras["elapsed_seconds"] = int(extras["elapsed_raw"]) if extras["elapsed_raw"].isdigit() else seconds(extras["elapsed"])
            rows[-1].update(extras)
    return rows


def parse_squeue(text):
    rows = []
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) not in (6, 13):
            continue
        jid, state, submit, start, user, workdir = [x.strip() for x in parts[:6]]
        state = normalize(state)
        rows.append({"job_id": jid, "state": state, "exit_code": None,
                     "derived_exit_code": None, "submit": timestamp(submit),
                     "start": None if state == "PENDING" else timestamp(start),
                     "end": None, "user": user, "workdir": workdir,
                     "raw": line[:1200], "source": "squeue"})
        if len(parts) == 13:
            extras = dict(zip(("partition", "job_name", "elapsed", "nodes", "node_list", "reason", "short_state"), (x.strip() for x in parts[6:])))
            extras["elapsed_seconds"] = seconds(extras["elapsed"])
            rows[-1].update(extras)
    return rows


def reconcile(job_id, account, accounting, queue, errors=()):
    parents = [r for r in accounting if r["job_id"] == job_id and r["user"] == account]
    parent = max(parents, key=lambda r: (r["submit"] or 0, r["start"] or 0, r["end"] or 0)) if parents else None
    queued = next((r for r in queue if r["job_id"] == job_id and r["user"] == account), None)
    # Queue is current; accounting may still describe the previous attempt.
    selected = dict(queued or parent or {"job_id": job_id, "state": "UNKNOWN",
                    "exit_code": None, "derived_exit_code": None, "submit": None,
                    "start": None, "end": None, "user": account, "workdir": "", "source": "none"})
    if queued and parent and parent["submit"] == queued["submit"] and parent["start"] == queued["start"]:
        selected["exit_code"] = parent["exit_code"]
        selected["derived_exit_code"] = parent["derived_exit_code"]
        for key in ("partition", "job_name", "nodes", "node_list"):
            if not selected.get(key):
                selected[key] = parent.get(key)
    steps = [r for r in accounting if r["job_id"].startswith(job_id + ".")
             and r["user"] in (account, "") and parent and r["submit"] in (parent["submit"], None)
             and (r["start"] or 0) >= (parent["start"] or 0)]
    selected["step_failures"] = [r for r in steps if r["state"] in FAILURES
                                  or nonzero(r["exit_code"])]
    selected["query_errors"] = list(errors)
    selected["account_mismatch"] = any(r["job_id"] == job_id and r["user"] != account
                                       for r in accounting + queue)
    return selected


def nonzero(code):
    if code is None or code == "":
        return False
    return bool(re.fullmatch(r"\d+:\d+", code)) and code != "0:0"


class Slurm:
    def __init__(self, runner=command):
        self.runner = runner

    def queue(self, account):
        return parse_squeue(self.runner([
            "squeue", "--noheader", "--array", "--user", account,
            "--format=" + QUEUE_FORMAT]))

    def recent(self, account, hours=24):
        return parse_sacct(self.runner(["sacct", "--noheader", "--parsable2", "--allocations", "--duplicates",
                                       "--user", account, "--starttime", f"now-{hours}hours", "--endtime", "now",
                                       "--format=" + ACCOUNT_FORMAT]))

    def details(self, job_id):
        if not JOB_ID.fullmatch(job_id):
            raise ValueError("无效 Job ID")
        raw = self.runner(["scontrol", "--oneliner", "show", "job", job_id]).strip()
        # scontrol does not escape spaces in values; split only at named fields.
        pieces = re.split(r"\s+(?=[A-Za-z][A-Za-z0-9_]*=)", raw)
        return dict(piece.split("=", 1) for piece in pieces if "=" in piece)

    def query(self, account, job_ids):
        results = {}
        if not job_ids:
            return results
        queue, queue_errors = [], []
        try:
            # Query the account once. --jobs for an already completed job can make
            # squeue return an error; a successful empty account queue is valid.
            queue = parse_squeue(self.runner([
                "squeue", "--noheader", "--array", "--user", account,
                "--format=" + QUEUE_FORMAT]))
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
            queue_errors.append(f"squeue: {str(exc)[:500]}")
        for offset in range(0, len(job_ids), 200):
            ids = job_ids[offset:offset + 200]
            if any(not JOB_ID.fullmatch(jid) for jid in ids):
                raise ValueError("只支持数字 Job ID 或单个数组元素（123_4）")
            joined = ",".join(ids)
            errors, accounting = list(queue_errors), []
            try:
                accounting = parse_sacct(self.runner([
                    "sacct", "--noheader", "--parsable2", "--duplicates", "--user", account,
                    "--jobs", joined,
                    "--format=" + ACCOUNT_FORMAT]))
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
                errors.append(f"sacct: {str(exc)[:500]}")
            for jid in ids:
                results[jid] = reconcile(jid, account, accounting, queue, errors)
        return results
