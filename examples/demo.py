"""Local controlled demo. No Slurm, SMTP, SSH or original computation mutations."""
import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from atomhpcmonitor.cli import config_at, spec_from
from atomhpcmonitor.monitor import initial_runtime, poll
from atomhpcmonitor.slurm import Slurm
from atomhpcmonitor.store import Store
from atomhpcmonitor.reports import enqueue_due

G16 = "Entering Gaussian System, Link 0=g16\n SCF Done: E(RB3LYP) = -100.1 A.U.\n Normal termination of Gaussian 16\n"
VASP = "vasp.6.3.0\n IBRION = 2; NSW = 200; ICHARG = 2; NELM = 200; EDIFF = 1E-5\n Iteration 1( 5)\n aborting loop because EDIFF is reached\n"
VASP_END = " General timing and accounting informations for this job:\n"
G16_PROGRESS = "Entering Gaussian System\n (Enter /g16/l502.exe)\n Cycle 12 Pass 1\n SCF Done: E(RB3LYP) = -100.123456 A.U. after 13 cycles\n Step number 7 out of a maximum of 100\n Maximum Force 0.002000 0.000450 NO\n RMS Force 0.001000 0.000300 NO\n Maximum Displacement 0.003000 0.001800 NO\n RMS Displacement 0.001000 0.001200 YES\n"


def run(output):
    output.mkdir(parents=True, exist_ok=False)
    config = config_at(Path(__file__).resolve().parent.parent / "config.example.json")
    config["settle_seconds"] = 0
    # Fixed demo clock: 21:35 Beijing, within the example 21:30 report window.
    now = datetime(2026, 10, 11, 21, 35, tzinfo=timezone(timedelta(hours=8))).timestamp()
    cases = [
        ("gaussian_user", "1001", "g16", "sp", "COMPLETED", G16, "SUCCEEDED"),
        ("gaussian_user", "1002", "g16", "opt", "COMPLETED", "Error termination via Lnk1e in /g16/l502.exe\n", "PROGRAM_FAILED"),
        ("gaussian_user", "1003", "g16", "sp", "RUNNING", "SCF Done: E = -10\n", "RUNNING"),
        ("hpc_user", "2001", "g16", "sp", "COMPLETED", G16, "SUCCEEDED"),
        ("hpc_user", "2002", "vasp", "relax", "COMPLETED", VASP + " reached required accuracy - stopping structural energy minimisation\n" + VASP_END, "SUCCEEDED"),
        ("hpc_user", "2003", "vasp", "relax", "COMPLETED", VASP + VASP_END, "GOAL_NOT_REACHED"),
        ("hpc_user", "2004", "vasp", "relax", "TIMEOUT", None, "TIMEOUT"),
        ("hpc_user", "2005", "g16", "sp", "COMPLETED", G16, "INSUFFICIENT"),
        ("hpc_user", "2006", "vasp", "relax", "OUT_OF_MEMORY", None, "OUT_OF_MEMORY"),
        ("hpc_user", "2007", "vasp", "relax", "RUNNING", VASP + " free energy TOTEN = -100.123 eV\n", "RUNNING"),
        ("hpc_user", "2008", "g16", "opt", "RUNNING", G16_PROGRESS, "RUNNING"),
        ("hpc_user", "2009", "g16", "opt", "COMPLETED", G16_PROGRESS + " Error termination via Lnk1e in /g16/l103.exe\n", "PROGRAM_FAILED"),
    ]
    report = {"kind": "controlled-local-demo", "smtp_sent": False, "accounts": {}, "input_hashes_unchanged": True}
    def stamp(value):
        return datetime.fromtimestamp(value, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    for account in ("gaussian_user", "hpc_user"):
        relevant = [case for case in cases if case[0] == account]
        store = Store(output / "state" / (account + ".sqlite3"))
        records, queue, hashes = [], [], {}
        for _, jid, program, task, state, text, expected in relevant:
            folder = output / "inputs" / account / jid
            folder.mkdir(parents=True)
            spec = spec_from({"job_id": jid, "program": program, "task": task, "workdir": str(folder),
                              "batch": "controlled-demo", "name": f"demo-{jid}", "adopt_existing": True}, account, config)
            if text is not None:
                path = Path(spec["log"])
                path.write_text(text, encoding="ascii")
                os.utime(path, (now - 600 if jid == "2005" else now, now - 600 if jid == "2005" else now))
                hashes[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
            store.add(spec, initial_runtime(spec))
            records.append(f"{jid}|{state}|{'0:0' if state in ('COMPLETED','RUNNING') else '1:0'}|0:0|{stamp(now - 200)}|{stamp(now - 100)}|{stamp(now - 20) if state != 'RUNNING' else 'Unknown'}|{account}|{folder}|demo-{jid}|demo-partition|00:01:20|1|demo-node01|80")
            if state == "RUNNING":
                queue.append(f"{jid}|{state}|{stamp(now - 200)}|{stamp(now - 100)}|{account}|{folder}|demo-partition|demo-{jid}|00:01:40|1|demo-node01|None|R")
        def runner(args):
            return "\n".join(queue if args[0] == "squeue" else records)
        with store.lock():
            first = poll(store, account, Slurm(runner), config, now=now)
            report_config = {**config, "reports": {**config["reports"], "enabled": True}}
            report_ids = enqueue_due(store, account, first, report_config, now)
        actual = {job["job_id"]: job["status"] for job in first["jobs"]}
        for case in relevant:
            if actual[case[1]] != case[-1]:
                raise AssertionError((case[1], actual[case[1]], case[-1]))
        count = len(store.outbox(account))
        store.close()
        store = Store(output / "state" / (account + ".sqlite3"))
        with store.lock():
            poll(store, account, Slurm(runner), config, now=now + 10)
            enqueue_due(store, account, first, report_config, now + 10)
        if len(store.outbox(account)) != count:
            raise AssertionError("Duplicate notification after restart")
        mails = store.outbox(account)
        mail_path = output / f"mail-preview-{account}.txt"
        mail_path.write_text("\n\n".join(row["subject"] + "\n\n" + row["body"] for row in mails), encoding="utf-8")
        report["accounts"][account] = {"statuses": actual, "outbox_count": count, "duplicate_after_restart": False,
                                         "scheduled_report_ids": report_ids,
                                         "batches": first["batches"], "mail_preview": str(mail_path)}
        if report_ids:
            scheduled = next(row for row in mails if row["id"] in report_ids)
            (output / f"progress-report-preview-{account}.txt").write_text(scheduled["subject"] + "\n\n" + scheduled["body"], encoding="utf-8")
            (output / f"progress-report-preview-{account}.html").write_text(scheduled["html_body"], encoding="utf-8")
        for path, digest in hashes.items():
            if hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest:
                raise AssertionError("Input was modified")
        store.close()
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True, help="使用一个尚不存在的新目录")
    run(parser.parse_args().output.resolve())
