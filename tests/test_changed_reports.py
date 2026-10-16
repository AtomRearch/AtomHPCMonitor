import contextlib
import copy
import io
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from atomhpcmonitor.reports import due_slots, enqueue_due, validate_settings
from atomhpcmonitor.store import Store
from scripts.configure_reports import configure


SETTINGS = {"enabled": True, "only_on_change": True, "accounts": ["hpc_user", "gaussian_user"],
            "programs": ["g16", "vasp"], "start_date": "2026-10-11", "end_date": "2026-11-11",
            "times": ["07:30", "13:00", "21:30"], "timezone": "UTC+08:00"}


def timestamp(day=11, hour=7, minute=30):
    return datetime(2026, 10, day, hour, minute,
                    tzinfo=datetime.fromisoformat("2026-10-11T00:00:00+08:00").tzinfo).timestamp()


def job(program="vasp", job_id="42"):
    return {"job_id": job_id, "account": "hpc_user", "name": "task-" + job_id,
            "workdir": "/work/" + job_id, "checked_at": "2026-10-13T07:30:00+08:00",
            "reason": "observed", "issues": [],
            "program": program, "task": "relax" if program == "vasp" else "opt",
            "status": "RUNNING", "program_status": "INSUFFICIENT", "program_reason": "未确认程序正常结束",
            "scheduler": {"state": "RUNNING", "submit": 1, "start": 2, "end": None, "elapsed": "00:30:00"},
            "progress": {"ionic_step": 86, "electronic_iteration": 2, "last_energy_eV": -100.125,
                         "optimization_step": 7, "scf_cycles": 12, "log_age_seconds": 0},
            "program_detail": {"termination": "unconfirmed", "electronic_converged": False,
                               "geometry_converged": False, "tail": []}}


def rendered(account, observation, programs, settings, now, slot=None, discovery=None):
    return {"subject": "test report", "body": "body", "html_body": "<p>report</p>",
            "payload": {"type": "progress-report", "programs": programs,
                        "jobs": copy.deepcopy(observation["jobs"]), "discovery": copy.deepcopy(discovery)}}


class ChangedReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "state.db"
        self.store = Store(self.path)
        self.jobs = [job()]
        self.render_patch = patch("atomhpcmonitor.reports.render_report", side_effect=rendered)
        self.render_patch.start()

    def tearDown(self):
        self.render_patch.stop()
        self.store.close()
        self.temp.cleanup()

    def enqueue(self, now=None, account="hpc_user", settings=None):
        with self.store.lock():
            return enqueue_due(self.store, account, {"jobs": self.jobs},
                               {"reports": settings or SETTINGS}, now or timestamp())

    def test_three_beijing_slots(self):
        for hour, minute in ((7, 30), (13, 0), (21, 30)):
            self.assertEqual(due_slots(SETTINGS, "hpc_user", timestamp(hour=hour, minute=minute))[0]["time"],
                             "{:02d}:{:02d}".format(hour, minute))
        self.assertEqual(due_slots(SETTINGS, "hpc_user", timestamp(hour=7, minute=29)), [])

    def test_empty_never_sends_even_first_slot(self):
        self.jobs = []
        self.assertEqual(self.enqueue(), [])
        self.assertEqual(self.store.outbox("hpc_user"), [])

    def test_first_snapshot_then_unchanged_after_restart(self):
        self.assertEqual(len(self.enqueue()), 1)
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.enqueue(timestamp(hour=13, minute=0)), [])
        self.assertEqual(len(self.store.outbox("hpc_user")), 1)

    def test_clock_tail_log_and_discovery_changes_do_not_send(self):
        self.enqueue()
        self.jobs[0]["checked_at"] = "new timestamp"
        self.jobs[0]["scheduler"].update(elapsed="06:38:30", elapsed_seconds=23910, reason="None")
        self.jobs[0]["progress"].update(log_age_seconds=100, log_updated_at="new", caught_up=True)
        self.jobs[0]["program_detail"].update(tail=["more text"], evidence=[{"line": 100}])
        self.jobs[0].update(terminal_snapshot=True, latest_query_errors=["query failed"])
        self.assertEqual(self.enqueue(timestamp(hour=13, minute=0)), [])

    def test_progress_changes_vasp_and_g16(self):
        for program, key in (("vasp", "ionic_step"), ("vasp", "electronic_iteration"),
                             ("vasp", "last_energy_eV"), ("g16", "optimization_step"),
                             ("g16", "scf_cycles"), ("g16", "frequency_count"), ("g16", "stage")):
            with self.subTest(program=program, key=key):
                self.jobs = [job(program, key)]
                # Unique report date per subcase so skipped slots cannot interfere.
                day = 11 + list(("ionic_step", "electronic_iteration", "last_energy_eV", "optimization_step",
                                 "scf_cycles", "frequency_count", "stage")).index(key)
                self.enqueue(timestamp(day=day))
                self.jobs[0]["progress"][key] = "new progress"
                self.assertEqual(len(self.enqueue(timestamp(day=day, hour=13, minute=0))), 1)

    def test_new_job_sends_while_old_job_stays_unchanged(self):
        self.enqueue()
        self.jobs.append(job("g16", "43"))
        self.assertEqual(len(self.enqueue(timestamp(hour=13, minute=0))), 1)

    def test_queue_run_completion_and_failure_changes(self):
        self.jobs[0]["status"] = "PENDING"
        self.jobs[0]["scheduler"]["state"] = "PENDING"
        self.enqueue()
        self.jobs[0] = job()
        self.assertEqual(len(self.enqueue(timestamp(hour=13, minute=0))), 1)
        self.jobs[0]["status"] = "SUCCEEDED"
        self.jobs[0]["scheduler"].update(state="COMPLETED", end=3, exit_code="0:0")
        self.assertEqual(len(self.enqueue(timestamp(hour=21, minute=30))), 1)
        self.jobs[0]["status"] = "SLURM_FAILED"
        self.jobs[0]["scheduler"]["derived_exit_code"] = "1:0"
        self.assertEqual(len(self.enqueue(timestamp(day=12))), 1)

    def test_running_program_error_counts_as_new(self):
        self.enqueue()
        self.jobs[0].update(program_status="PROGRAM_FAILED", program_reason="SCF error")
        self.assertEqual(len(self.enqueue(timestamp(hour=13, minute=0))), 1)

    def test_outage_alone_does_not_send(self):
        self.enqueue()
        self.jobs[0]["status"] = "INSUFFICIENT"
        self.jobs[0]["scheduler"].update(state="UNKNOWN", query_errors=["unavailable"])
        self.assertEqual(self.enqueue(timestamp(hour=13, minute=0)), [])
        self.jobs = [job()]
        self.assertEqual(self.enqueue(timestamp(hour=21, minute=30)), [])

    def test_removed_jobs_do_not_send(self):
        self.jobs.append(job("g16", "43"))
        self.enqueue()
        self.jobs.pop()
        self.assertEqual(self.enqueue(timestamp(hour=13, minute=0)), [])

    def test_other_jobs_progress_during_outage_does_not_erase_baseline(self):
        self.jobs.append(job("g16", "43"))
        self.enqueue()
        self.jobs[0]["status"] = "INSUFFICIENT"
        self.jobs[0]["scheduler"].update(state="UNKNOWN", query_errors=["unavailable"])
        self.jobs[1]["progress"]["optimization_step"] = 8
        self.assertEqual(len(self.enqueue(timestamp(hour=13, minute=0))), 1)
        self.jobs[0] = job()
        self.assertEqual(self.enqueue(timestamp(hour=21, minute=30)), [])

    def test_pending_failed_unknown_mail_is_not_duplicated(self):
        mid = self.enqueue()[0]
        for day, status in enumerate(("pending", "failed", "unknown", "accepted"), 12):
            self.store.mail_state(mid, status, timestamp(day=day))
            self.assertEqual(self.enqueue(timestamp(day=day)), [])

    def test_accepted_manual_report_is_baseline(self):
        payload = rendered("hpc_user", {"jobs": self.jobs}, SETTINGS["programs"], SETTINGS, 0)["payload"]
        mid = self.store.enqueue_report("hpc_user", "manual-report:hpc_user:one", "manual", "body", payload, 0)
        self.store.mail_state(mid, "accepted", 0)
        self.assertEqual(self.enqueue(), [])

    def test_unsent_manual_preview_does_not_hide_progress(self):
        payload = rendered("hpc_user", {"jobs": self.jobs}, SETTINGS["programs"], SETTINGS, 0)["payload"]
        self.store.enqueue_report("hpc_user", "manual-report:hpc_user:one", "manual", "body", payload, 0)
        self.assertEqual(len(self.enqueue()), 1)

    def test_skipped_slot_stays_consumed_after_restart(self):
        self.enqueue()
        self.assertEqual(self.enqueue(timestamp(hour=13, minute=0)), [])
        self.store.close()
        self.store = Store(self.path)
        self.jobs[0]["progress"]["ionic_step"] = 87
        self.assertEqual(self.enqueue(timestamp(hour=13, minute=5)), [])
        self.assertEqual(len(self.enqueue(timestamp(hour=21, minute=30))), 1)

    def test_accounts_isolate_but_program_filters_share_job_baselines(self):
        self.enqueue()
        self.assertEqual(len(self.enqueue(account="gaussian_user")), 1)
        self.assertEqual(self.enqueue(timestamp(hour=13, minute=0), settings={**SETTINGS, "programs": ["vasp"]}), [])
        self.jobs.append(job("g16", "43"))
        self.assertEqual(self.enqueue(timestamp(hour=21, minute=30), settings={**SETTINGS, "programs": ["vasp"]}), [])

    def test_report_only_contains_changed_jobs_in_payload_plain_and_html(self):
        # Read the original function while temporarily removing our rendering stub.
        self.render_patch.stop()
        from atomhpcmonitor.reports import render_report
        try:
            old = job("vasp", "completed-old")
            old["status"] = "SUCCEEDED"
            old["scheduler"].update(state="COMPLETED", end=3, exit_code="0:0")
            old["program_status"] = "SUCCEEDED"
            self.jobs.append(old)
            self.enqueue()
            self.jobs[0]["progress"]["ionic_step"] = 87
            mid = self.enqueue(timestamp(hour=13, minute=0))[0]
            row = next(r for r in self.store.outbox("hpc_user") if r["id"] == mid)
            payload = json.loads(self.store.db.execute("SELECT payload FROM events WHERE id=?", (row["event_id"],)).fetchone()[0])
            self.assertEqual([j["job_id"] for j in payload["jobs"]], ["42"])
            self.assertEqual(payload["counts"], {"RUNNING": 1})
            for text in (row["subject"], row["body"], row["html_body"]):
                self.assertNotIn("completed-old", text)
            self.assertIn("本次新增 / 有进展", row["html_body"])
            full = render_report("hpc_user", {"jobs": self.jobs}, SETTINGS["programs"], SETTINGS, timestamp())
            self.assertIn("completed-old", full["body"])
        finally:
            self.render_patch.start()

    def test_completion_notification_is_shared_with_report(self):
        completed = job()
        completed["status"] = "SUCCEEDED"
        completed["scheduler"].update(state="COMPLETED", end=3, exit_code="0:0")
        spec = {"account": "hpc_user", "job_id": "42", "batch": "test", "log": "/work/42/OUTCAR"}
        self.store.add(spec, {})
        self.store.save(spec, {}, completed, {"key": "completion:42", "subject": "done", "body": "done"}, timestamp())
        self.jobs = [completed, job("g16", "43")]
        mid = self.enqueue()[0]
        row = next(r for r in self.store.outbox("hpc_user") if r["id"] == mid)
        payload = json.loads(self.store.db.execute("SELECT payload FROM events WHERE id=?", (row["event_id"],)).fetchone()[0])
        self.assertEqual([j["job_id"] for j in payload["jobs"]], ["43"])
        self.assertEqual(self.enqueue(timestamp(hour=13, minute=0)), [])

    def test_pending_completion_retry_keeps_ownership_of_snapshot(self):
        spec = {"account": "hpc_user", "job_id": "42", "batch": "test", "log": "/work/42/OUTCAR"}
        self.store.add(spec, {})
        self.store.save(spec, {}, self.jobs[0], {"key": "event:42", "subject": "event", "body": "event"}, timestamp())
        for day, status in enumerate(("pending", "failed", "unknown", "accepted"), 12):
            self.store.mail_state(1, status, timestamp(day=day))
            self.assertEqual(self.enqueue(timestamp(day=day)), [])

    def test_more_than_100_changes_are_reported_in_later_slots(self):
        self.jobs = [job(job_id=str(n)) for n in range(105)]
        first = self.enqueue()[0]
        second = self.enqueue(timestamp(hour=13, minute=0))[0]
        payloads = [json.loads(r[0]) for r in self.store.db.execute("SELECT payload FROM events ORDER BY id")]
        self.assertEqual([len(p["jobs"]) for p in payloads], [100, 5])
        self.assertEqual(len({j["job_id"] for p in payloads for j in p["jobs"]}), 105)
        self.assertNotEqual(first, second)
        self.assertEqual(self.enqueue(timestamp(hour=21, minute=30)), [])

    def test_old_full_report_only_marks_displayed_100_jobs(self):
        self.jobs = [job(job_id=str(n)) for n in range(105)]
        payload = {"type": "progress-report", "programs": SETTINGS["programs"], "jobs": copy.deepcopy(self.jobs)}
        self.store.enqueue_report("hpc_user", "report:hpc_user:legacy", "old full", "body", payload, 0)
        self.enqueue()
        payload = json.loads(self.store.db.execute("SELECT payload FROM events ORDER BY id DESC LIMIT 1").fetchone()[0])
        self.assertEqual(len(payload["jobs"]), 5)

    def test_unchanged_discovery_skips_do_not_reappear(self):
        discovery = {"candidates": [], "registered": [], "skipped": [{"job_id": "DOS-old", "slurm_state": "COMPLETED",
                     "reason": "unsupported", "workdir": "/work/DOS"}], "errors": []}
        def enqueue(now):
            return enqueue_due(self.store, "hpc_user", {"jobs": self.jobs}, {"reports": SETTINGS}, now, discovery)
        enqueue(timestamp())
        self.jobs[0]["progress"]["ionic_step"] += 1
        enqueue(timestamp(hour=13, minute=0))
        payloads = [json.loads(r[0]) for r in self.store.db.execute("SELECT payload FROM events ORDER BY id")]
        self.assertEqual(len(payloads[0]["discovery"]["skipped"]), 1)
        self.assertEqual(payloads[1]["discovery"]["skipped"], [])
        discovery["skipped"][0]["reason"] = "changed reason"
        self.jobs[0]["progress"]["ionic_step"] += 1
        enqueue(timestamp(hour=21, minute=30))
        payload = json.loads(self.store.db.execute("SELECT payload FROM events ORDER BY id DESC LIMIT 1").fetchone()[0])
        self.assertEqual(payload["discovery"]["skipped"][0]["reason"], "changed reason")

    def test_setting_requires_boolean(self):
        with self.assertRaises(ValueError):
            validate_settings({**SETTINGS, "only_on_change": "true"})

    def test_config_update_preserves_mail_dates_and_state(self):
        path = Path(self.temp.name) / "config.json"
        original = {"reports": {**SETTINGS, "enabled": False}, "smtp": {"password": "test-secret"}, "state_dir": "old-state"}
        path.write_text(json.dumps(original), encoding="utf-8")
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            configure(path, ["07:30", "13:00", "21:30"])
        updated = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(updated["smtp"], original["smtp"])
        self.assertEqual(updated["state_dir"], "old-state")
        self.assertEqual(updated["reports"]["end_date"], SETTINGS["end_date"])
        self.assertTrue(updated["reports"]["enabled"])
        self.assertNotIn("test-secret", captured.getvalue())
        before = path.read_bytes()
        with self.assertRaises(ValueError):
            configure(path, ["24:00"])
        self.assertEqual(path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
