import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from atomhpcmonitor.cli import spec_from, main
from atomhpcmonitor.discovery import discover, incar_values, vasp_task
from atomhpcmonitor.mail import deliver
from atomhpcmonitor.monitor import poll
from atomhpcmonitor.reports import due_slots, enqueue_due, render_report, validate_settings
from atomhpcmonitor.slurm import Slurm
from atomhpcmonitor.store import Store

CONFIG = {"settle_seconds": 0, "unknown_seconds": 10, "read_bytes": 8388608,
          "poll_seconds": 300, "smtp": {"enabled": False},
          "accounts": {"hpc_user": {"programs": ["g16", "vasp"], "default_logs": {"g16": "slurm-{job_id}.out", "vasp": "OUTCAR"}}}}
REPORTS = {"enabled": True, "accounts": ["hpc_user"], "programs": ["vasp"],
           "start_date": "2026-10-11", "end_date": "2026-11-11",
           "times": ["12:00", "20:00"], "timezone": "UTC+08:00", "catch_up_minutes": 60}
OUTCAR = "vasp.6.3.0\n IBRION = 2; NSW = 400; NELM = 200; ICHARG = 2; EDIFF = 1E-5\n Iteration 37( 5)\n free  energy   TOTEN  = -100.123 eV\n"


def utc(text):
    return datetime.fromisoformat(text).timestamp()


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.folder = self.root / "calc A"
        self.folder.mkdir()
        self.now = utc("2026-10-11T20:05:00+08:00")
        self.submit = "2026-10-11T10:00:00"
        self.start = "2026-10-11T10:01:00"
        self.script = self.folder / "vasp.slurm"
        self.script.write_text('#!/bin/bash\nmodule load vasp-6.3.0\ncd "${SLURM_SUBMIT_DIR:?Submit here}"\nsrun --mpi=pmi2 vasp_std\n')
        (self.folder / "INCAR").write_text("NSW=400; IBRION=2\nICHARG=2 # self-consistent\n")
        (self.folder / "OUTCAR").write_text(OUTCAR)
        os.utime(self.folder / "OUTCAR", (self.now, self.now))
        self.store = Store(self.root / "hpc_user.sqlite3")
        self.user = "hpc_user"
        self.state = "RUNNING"
        self.detail_submit = self.submit
        self.detail_start = self.start

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def runner(self, args):
        if args[0] == "squeue":
            return f"42|{self.state}|{self.submit}|{self.start}|{self.user}|{self.folder}\n"
        if args[0] == "scontrol":
            return f"JobId=42 JobName=sample-A_cont UserId={self.user}(1000) JobState={self.state} SubmitTime={self.detail_submit} StartTime={self.detail_start} WorkDir={self.folder} Command={self.script} StdOut={self.folder}/slurm-%j.out StdErr={self.folder}/slurm-%j.err"
        if args[0] == "sacct":
            return f"42|{self.state}|0:0|0:0|{self.submit}|{self.start}|Unknown|{self.user}|{self.folder}\n"
        raise AssertionError(args)

    def find(self, register=False):
        return discover(self.store, "hpc_user", Slurm(self.runner), CONFIG, spec_from, register)

    def test_preview_does_not_register(self):
        result = self.find()
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(self.store.jobs("hpc_user"), [])
        self.assertEqual(result["candidates"][0]["task"], "relax")
        self.assertTrue(result["candidates"][0]["adopt_existing"])
        self.assertTrue(result["candidates"][0]["aux_logs"][0].endswith("slurm-42.out"))

    def test_register_idempotent_and_progress(self):
        self.assertEqual(self.find(True)["registered"], ["42"])
        self.assertEqual(self.find(True)["registered"], [])
        job = poll(self.store, "hpc_user", Slurm(self.runner), CONFIG, now=self.now)["jobs"][0]
        self.assertEqual(job["status"], "RUNNING")
        self.assertEqual(job["progress"]["ionic_step"], 37)
        self.assertEqual(job["progress"]["ionic_limit"], 400)
        self.assertAlmostEqual(job["progress"]["last_energy_eV"], -100.123)

    def test_other_account_ignored(self):
        self.user = "gaussian_user"
        self.assertEqual(self.find(True)["candidates"], [])

    def test_running_single_point_report(self):
        (self.folder / "INCAR").write_text("NSW=0; IBRION=-1; ICHARG=2\n")
        (self.folder / "OUTCAR").write_text(OUTCAR.replace("IBRION = 2; NSW = 400", "IBRION = -1; NSW = 0"))
        os.utime(self.folder / "OUTCAR", (self.now, self.now))
        self.find(True)
        observation = poll(self.store, "hpc_user", Slurm(self.runner), CONFIG, now=self.now)
        job = observation["jobs"][0]
        self.assertEqual(job["task"], "sp")
        self.assertEqual(job["status"], "RUNNING")
        self.assertFalse(job["notify"])
        report = render_report("hpc_user", observation, ["vasp"], REPORTS, self.now)
        self.assertIn("不进行离子优化", report["body"])
        self.assertIn("运行中，尚未确认程序正常结束", report["body"])
        self.assertIn("当前电子迭代: 5 / NELM上限 200", report["body"])
        self.assertNotIn("当前已读离子步", report["body"])

    def test_stale_outcar_not_adopted(self):
        os.utime(self.folder / "OUTCAR", (1, 1))
        result = self.find(True)
        self.assertEqual(result["registered"], [])
        self.assertIn("旧日志", result["skipped"][0]["reason"])

    def test_pending_baselines_old_log(self):
        self.state = "PENDING"
        self.detail_start = "Unknown"
        self.find(True)
        job = self.store.jobs("hpc_user")[0]
        self.assertFalse(job["spec"]["adopt_existing"])
        self.assertGreater(job["runtime"]["cursor"]["offset"], 0)

    def test_changed_start_or_submit_not_adopted(self):
        self.detail_start = "2026-10-11T10:02:00"
        self.assertEqual(self.find(True)["registered"], [])
        self.detail_start = self.start
        self.detail_submit = "2026-10-11T09:59:00"
        self.assertEqual(self.find(True)["registered"], [])

    def test_unreadable_script(self):
        self.script.unlink()
        self.assertEqual(self.find(True)["registered"], [])

    def test_echo_or_module_not_execution(self):
        self.script.write_text('module load vasp_std\necho "srun vasp_std"\n')
        self.assertEqual(self.find(True)["registered"], [])

    def test_directory_changes_need_manual_mapping(self):
        self.script.write_text("cd /another/calculation\nsrun vasp_std\n")
        self.assertEqual(self.find(True)["registered"], [])
        self.script.write_text("pushd /another/calculation\nsrun vasp_std\n")
        self.assertEqual(self.find(True)["registered"], [])

    def test_unsupported_incar_skipped(self):
        (self.folder / "INCAR").write_text("IBRION=0; NSW=400\n")
        self.assertEqual(self.find(True)["registered"], [])

    def test_parenthesized_incar_annotation_does_not_skip_live_job(self):
        path = self.folder / "INCAR"
        path.write_text("NSW = 100          (Max ionic steps)\nIBRION = 2 (Optimization)\nICHARG = 2 (SCF)\nALGO = Normal (Electronic algorithm)\n")
        before = path.read_bytes()
        result = self.find(True)
        self.assertEqual(result["registered"], ["42"])
        self.assertEqual(result["candidates"][0]["task"], "relax")
        self.assertEqual(path.read_bytes(), before)

    def test_disabled_job_not_rediscovered(self):
        self.find(True)
        self.store.db.execute("UPDATE jobs SET enabled=0")
        self.store.db.commit()
        self.assertEqual(self.find(True)["registered"], [])

    def test_discovery_query_error_recorded(self):
        def broken(args):
            raise RuntimeError("scheduler unavailable")
        result = discover(self.store, "hpc_user", Slurm(broken), CONFIG, spec_from, True)
        self.assertTrue(result["errors"])
        self.assertEqual(self.store.jobs("hpc_user"), [])

    def test_two_queue_jobs_sharing_outcar_both_skipped(self):
        def runner(args):
            if args[0] == "squeue":
                value = self.runner(args)
                return value + value.replace("42|", "43|")
            if args[0] == "scontrol" and args[-1] == "43":
                return self.runner(args).replace("JobId=42", "JobId=43")
            return self.runner(args)
        result = discover(self.store, "hpc_user", Slurm(runner), CONFIG, spec_from, True)
        self.assertEqual(result["registered"], [])
        self.assertEqual(len(result["skipped"]), 2)

    def test_cli_dry_run_discovers_without_writes(self):
        path = self.root / "config.json"
        path.write_text(json.dumps({**CONFIG, "state_dir": str(self.root)}))
        output = io.StringIO()
        with patch("atomhpcmonitor.cli.current_user", return_value="hpc_user"), patch("atomhpcmonitor.cli.Slurm", return_value=Slurm(self.runner)), contextlib.redirect_stdout(output):
            status = main(["--config", str(path), "--account", "hpc_user", "poll", "--discover", "vasp", "--dry-run"])
        result = json.loads(output.getvalue())
        self.assertEqual(status, 0)
        self.assertEqual(result["jobs"][0]["progress"]["ionic_step"], 37)
        self.assertEqual(self.store.jobs("hpc_user"), [])

    def test_manual_report_preview_and_send_only_one_mail(self):
        self.find(True)
        self.store.enqueue_report("hpc_user", "older-event", "old", "old", {}, self.now)
        path = self.root / "config.json"
        path.write_text(json.dumps({**CONFIG, "state_dir": str(self.root), "reports": {**REPORTS, "enabled": False}}))
        base = ["--config", str(path), "--account", "hpc_user", "report", "--program", "vasp"]
        sent = []
        def fake_deliver(store, account, settings, now, only_ids=None):
            return deliver(store, account, settings, now, sender=lambda row, s: sent.append(row["subject"]), only_ids=only_ids)
        with patch("atomhpcmonitor.cli.current_user", return_value="hpc_user"), patch("atomhpcmonitor.cli.Slurm", return_value=Slurm(self.runner)), patch("atomhpcmonitor.cli.deliver", side_effect=fake_deliver), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(base + ["--html-output", str(self.root / "preview.html")]), 0)
            self.assertIn("计算进展", (self.root / "preview.html").read_text(encoding="utf-8"))
            self.assertEqual(len(self.store.outbox("hpc_user")), 1)
            self.assertEqual(main(base + ["--send"]), 0)
        self.assertEqual(len(sent), 1)
        self.assertEqual(self.store.outbox("hpc_user")[0]["status"], "pending")
        self.assertEqual(self.store.outbox("hpc_user")[1]["status"], "accepted")


class ScheduleTests(unittest.TestCase):
    def test_parenthesized_annotations_preserve_unsupported_task_filters(self):
        for values in ({"IMAGES": "5 (NEB)"}, {"ICHARG": "11 (DOS)"},
                       {"IBRION": "0 (MD)", "NSW": "100 (steps)"},
                       {"LEPSILON": ".TRUE. (dielectric)"}, {"LCALCEPS": "TRUE (response)"},
                       {"ALGO": "GW0 (GW)"}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                vasp_task(values)

    def test_integer_syntax_remains_strict(self):
        for value in ("100.5 (steps)", "100junk", "100 200", "1e2", "100 (first) (second)", "100\n(other)"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                vasp_task({"NSW": value, "IBRION": "2"})

    def test_timezone_and_two_slots(self):
        noon = utc("2026-10-11T04:00:00+00:00")
        evening = utc("2026-10-11T12:00:00+00:00")
        self.assertEqual(due_slots(REPORTS, "hpc_user", noon)[0]["time"], "12:00")
        self.assertEqual(due_slots(REPORTS, "hpc_user", evening)[0]["time"], "20:00")

    def test_inclusive_date_range_and_outside(self):
        self.assertEqual(len(due_slots(REPORTS, "hpc_user", utc("2026-11-11T20:00:00+08:00"))), 1)
        self.assertEqual(due_slots(REPORTS, "hpc_user", utc("2026-11-12T20:00:00+08:00")), [])
        self.assertEqual(due_slots(REPORTS, "hpc_user", utc("2026-10-10T20:00:00+08:00")), [])

    def test_before_slot_and_expired_window(self):
        self.assertEqual(due_slots(REPORTS, "hpc_user", utc("2026-10-11T19:59:59+08:00")), [])
        self.assertEqual(due_slots(REPORTS, "hpc_user", utc("2026-10-11T21:00:01+08:00")), [])
        self.assertEqual(len(due_slots(REPORTS, "hpc_user", utc("2026-10-11T20:05:00+08:00"))), 1)

    def test_next_day_catchup_after_end_date(self):
        settings = {**REPORTS, "times": ["23:50"], "end_date": "2026-10-11"}
        self.assertEqual(due_slots(settings, "hpc_user", utc("2026-10-12T00:10:00+08:00"))[0]["date"], "2026-10-11")

    def test_wrong_account_and_disabled(self):
        now = utc("2026-10-11T20:00:00+08:00")
        self.assertEqual(due_slots(REPORTS, "gaussian_user", now), [])
        self.assertEqual(due_slots({**REPORTS, "enabled": False}, "hpc_user", now), [])

    def test_editable_format_validation(self):
        for values in ({"times": ["24:00"]}, {"times": ["8:00"]}, {"times": ["20.00"]},
                       {"times": "20:00"}, {"end_date": "2026-10-01"}, {"timezone": "UTC+99:00"}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                validate_settings({**REPORTS, **values})

    def test_each_slot_once_after_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.sqlite3"
            store = Store(path)
            observation = {"jobs": [], "batches": {}}
            config = {"reports": {**REPORTS, "only_on_change": False}}
            noon = utc("2026-10-11T12:00:00+08:00")
            with store.lock():
                self.assertEqual(len(enqueue_due(store, "hpc_user", observation, config, noon)), 1)
            store.close()
            store = Store(path)
            try:
                with store.lock():
                    self.assertEqual(enqueue_due(store, "hpc_user", observation, config, noon + 300), [])
                    self.assertEqual(len(enqueue_due(store, "hpc_user", observation, config, noon + 8 * 3600)), 1)
                self.assertEqual(len(store.outbox("hpc_user")), 2)
            finally:
                store.close()

    def test_incar_comments_defaults_and_special_tasks(self):
        values = incar_values("# NSW=100\nNSW=200; IBRION=2 ! comment\n")
        self.assertEqual(vasp_task(values), "relax")
        self.assertEqual(vasp_task({}), "sp")
        for values in ({"IMAGES": "5"}, {"ICHARG": "11"}, {"IBRION": "0", "NSW": "200"}, {"ALGO": "GW0"}):
            with self.assertRaises(ValueError):
                vasp_task(values)


if __name__ == "__main__":
    unittest.main()
