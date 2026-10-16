import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from atomhpcmonitor.cli import main, spec_from
from atomhpcmonitor.logs import snapshot, read_new
from atomhpcmonitor.mail import deliver, send, DeliveryError
from atomhpcmonitor.monitor import initial_runtime, poll, observe
from atomhpcmonitor.parsers import fresh, feed, program_result
from atomhpcmonitor.slurm import Slurm, parse_sacct, parse_squeue, reconcile
from atomhpcmonitor.store import Store

G16_SP = "Entering Gaussian System, Link 0=g16\n SCF Done: E(RB3LYP) = -100.1 A.U.\n Normal termination of Gaussian 16 at Sun Oct 11 20:00:00 2026.\n"
G16_OPT = G16_SP.replace(" Normal termination", " Optimization completed.\n Normal termination")
G16_ERROR = "Entering Gaussian System\n Convergence failure -- run terminated.\n Error termination via Lnk1e in /g16/l502.exe\n"
VASP_HEADER = "vasp.6.3.0\n IBRION = 2; NSW = 80; ICHARG = 2; NELM = 60; EDIFF = 0.1E-05\n"
VASP_SCF = " Iteration    1(   1)\n aborting loop because EDIFF is reached\n"
VASP_END = " General timing and accounting informations for this job:\n"
VASP_RELAX = VASP_HEADER + VASP_SCF + " reached required accuracy - stopping structural energy minimisation\n" + VASP_END
CONFIG = {"settle_seconds": 0, "unknown_seconds": 10, "read_bytes": 8388608,
          "poll_seconds": 300, "smtp": {"enabled": False},
          "accounts": {"gaussian_user": {"programs": ["g16"], "default_logs": {"g16": "1.log"}},
                       "hpc_user": {"programs": ["g16", "vasp"], "default_logs": {"g16": "slurm-{job_id}.out", "vasp": "OUTCAR"}}}}


def parsed(text, program="g16", task="sp", **kwargs):
    f = fresh(program)
    for line in text.splitlines():
        feed(f, line)
    return program_result(f, task, **kwargs)


class ParserTests(unittest.TestCase):
    def test_g16_sp(self):
        self.assertEqual(parsed(G16_SP)[0], "SUCCEEDED")

    def test_g16_opt(self):
        self.assertEqual(parsed(G16_OPT, task="opt")[0], "SUCCEEDED")
        self.assertEqual(parsed(G16_SP, task="opt")[0], "GOAL_NOT_REACHED")

    def test_g16_error_link(self):
        result = parsed(G16_ERROR)
        self.assertEqual(result[0], "PROGRAM_FAILED")
        self.assertIn("l502", result[1])

    def test_g16_unfinished_not_failed(self):
        self.assertEqual(parsed(" SCF Done: E = -1\n")[0], "INSUFFICIENT")

    def test_g16_old_invocation_does_not_finish_new_one(self):
        self.assertEqual(parsed(G16_SP + "Entering Gaussian System\n SCF Done: E = -2\n")[0], "INSUFFICIENT")

    def test_g16_link1_unfinished_and_failure(self):
        prefix = G16_SP + " Link1: Proceeding to internal job step number 2.\n"
        self.assertEqual(parsed(prefix)[0], "INSUFFICIENT")
        self.assertEqual(parsed(prefix + " Error termination via Lnk1e in l716.exe\n")[0], "PROGRAM_FAILED")

    def test_g16_expected_links(self):
        self.assertEqual(parsed(G16_SP, expected_links=2)[0], "INSUFFICIENT")
        self.assertEqual(parsed(G16_SP + "Proceeding to internal job step number 2\n SCF Done: E = -1\n Normal termination of Gaussian 16\n", expected_links=2)[0], "SUCCEEDED")

    def test_g16_opt_must_belong_to_current_link(self):
        text = G16_OPT + "Proceeding to internal job step number 2\n SCF Done: E = -1\n Normal termination of Gaussian 16\n"
        self.assertEqual(parsed(text, task="opt")[0], "GOAL_NOT_REACHED")

    def test_g16_opt_freq_across_links(self):
        text = G16_OPT + "Proceeding to internal job step number 2\n SCF Done: E = -1\n Frequencies -- 1 2 3\n Zero-point correction= 1\n Normal termination of Gaussian 16\n"
        self.assertEqual(parsed(text, task="opt_freq", expected_links=2)[0], "SUCCEEDED")

    def test_g16_frequencies_and_target(self):
        text = "Entering Gaussian System\n Optimization completed.\n Frequencies -- -5.0 100.0 200.0\n Zero-point correction= 0.01\n Normal termination of Gaussian 16\n"
        self.assertEqual(parsed(text, task="opt_freq")[0], "SUCCEEDED")
        self.assertEqual(parsed(text, task="freq", expected_imaginary=0)[0], "GOAL_NOT_REACHED")
        result = parsed(text, task="freq", expected_imaginary=0, tolerance=10)
        self.assertEqual(result[0], "SUCCEEDED")
        self.assertEqual(result[2]["imaginary_count"], 0)

    def test_g16_frequency_truncated(self):
        self.assertEqual(parsed(" Frequencies -- 1 2 3\n Normal termination of Gaussian 16\n", task="freq")[0], "INSUFFICIENT")

    def test_vasp_relax(self):
        self.assertEqual(parsed(VASP_RELAX, "vasp", "relax")[0], "SUCCEEDED")

    def test_vasp_sp(self):
        text = VASP_HEADER.replace("IBRION = 2; NSW = 80", "IBRION = -1; NSW = 0") + VASP_SCF + VASP_END
        self.assertEqual(parsed(text, "vasp", "sp")[0], "SUCCEEDED")
        self.assertEqual(parsed(text, "vasp", "relax")[0], "INSUFFICIENT")

    def test_vasp_electronic_does_not_imply_ionic(self):
        self.assertEqual(parsed(VASP_HEADER + VASP_SCF + VASP_END, "vasp", "relax")[0], "GOAL_NOT_REACHED")

    def test_vasp_last_electronic_step_only(self):
        text = VASP_HEADER + VASP_SCF + " Iteration    2(  60)\n" + VASP_END
        self.assertEqual(parsed(text, "vasp", "relax")[0], "GOAL_NOT_REACHED")

    def test_vasp_earlier_unconverged_step(self):
        text = VASP_HEADER + " Iteration 1( 60)\n Iteration 2( 2)\n aborting loop because EDIFF is reached\n reached required accuracy - stopping structural energy minimisation\n" + VASP_END
        self.assertEqual(parsed(text, "vasp", "relax")[0], "NEEDS_REVIEW")

    def test_vasp_new_run_resets_old_markers(self):
        self.assertEqual(parsed(VASP_RELAX + VASP_HEADER + " Iteration 1( 1)\n", "vasp", "relax")[0], "INSUFFICIENT")

    def test_vasp_fatal_error(self):
        self.assertEqual(parsed(VASP_HEADER + " ZBRENT: fatal error in bracketing\n", "vasp", "relax")[0], "PROGRAM_FAILED")

    def test_vasp_unsupported_non_scf(self):
        self.assertEqual(parsed(VASP_RELAX.replace("ICHARG = 2", "ICHARG = 11"), "vasp", "relax")[0], "INSUFFICIENT")

    def test_unknown_task_not_success(self):
        self.assertEqual(parsed(VASP_RELAX, "vasp", "md")[0], "INSUFFICIENT")
        self.assertEqual(parsed(G16_SP, task="scan")[0], "INSUFFICIENT")


class TempTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / "state.sqlite3")
        self.now = 1800000000.0

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def spec(self, jid="123", account="gaussian_user", program="g16", task="sp", adopt=True, log=None):
        return spec_from({"job_id": jid, "program": program, "task": task, "workdir": str(self.root),
                          "batch": "demo", "adopt_existing": adopt, "log": log}, account, CONFIG)

    def scheduler(self, state="COMPLETED", **kwargs):
        return {"job_id": "123", "state": state, "exit_code": "0:0", "derived_exit_code": "0:0",
                "user": "gaussian_user", "submit": self.now - 120, "start": self.now - 100,
                "end": self.now - 20, "workdir": str(self.root), "query_errors": [], "step_failures": [], **kwargs}

    def register(self, text=G16_SP, **kwargs):
        spec = self.spec(**kwargs)
        Path(spec["log"]).write_text(text, encoding="ascii")
        os.utime(spec["log"], (self.now, self.now))
        self.store.add(spec, initial_runtime(spec))
        return spec

    def do_poll(self, record=None, account="gaussian_user", now=None, dry=False):
        record = record or self.scheduler()
        class FakeSlurm:
            def query(inner, target, ids):
                return {jid: record for jid in ids}
        return poll(self.store, account, FakeSlurm(), CONFIG, now=self.now if now is None else now, dry_run=dry)


class MonitorTests(TempTests):
    def test_completion_persistence_and_dedup(self):
        self.register()
        self.assertEqual(self.do_poll()["jobs"][0]["status"], "SUCCEEDED")
        self.store.close()
        self.store = Store(self.root / "state.sqlite3")
        self.do_poll()
        self.assertEqual(len(self.store.outbox("gaussian_user")), 1)
        self.assertIn("批次: demo", self.store.outbox("gaussian_user")[0]["body"])

    def test_accepted_manual_completion_report_prevents_later_event_duplicate(self):
        self.register()
        observation = self.do_poll(dry=True)
        payload = {"type": "progress-report", "programs": ["g16"], "jobs": observation["jobs"]}
        mid = self.store.enqueue_report("gaussian_user", "manual-report:gaussian_user:first", "manual", "body", payload, self.now)
        self.store.mail_state(mid, "accepted", self.now)
        result = self.do_poll()["jobs"][0]
        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertFalse(result["notify"])
        self.assertEqual(result["notification_suppressed"], "already-reported")
        self.assertEqual(len(self.store.outbox("gaussian_user")), 1)

    def test_unsent_manual_report_does_not_suppress_completion_event(self):
        self.register()
        payload = {"type": "progress-report", "programs": ["g16"], "jobs": self.do_poll(dry=True)["jobs"]}
        self.store.enqueue_report("gaussian_user", "manual-report:gaussian_user:first", "manual", "body", payload, self.now)
        self.assertTrue(self.do_poll()["jobs"][0]["notify"])
        self.assertEqual(len(self.store.outbox("gaussian_user")), 2)

    def test_completion_html_does_not_repeat_card_as_full_body(self):
        self.register()
        result = self.do_poll()["jobs"][0]
        row = self.store.outbox("gaussian_user")[0]
        self.assertEqual(row["html_body"].count(result["reason"]), 1)
        self.assertIn("关键证据", row["html_body"])
        self.assertIn("Slurm: COMPLETED", row["body"])
        self.assertNotIn("Slurm: COMPLETED", row["html_body"])

    def test_running_error_not_terminal_failure(self):
        self.register(G16_ERROR)
        result = self.do_poll(self.scheduler("RUNNING"))["jobs"][0]
        self.assertEqual(result["status"], "RUNNING")
        self.assertEqual(result["program_status"], "PROGRAM_FAILED")
        self.assertEqual(len(self.store.outbox("gaussian_user")), 0)

    def test_pending_old_success_ignored(self):
        self.register()
        result = self.do_poll(self.scheduler("PENDING", start=None))["jobs"][0]
        self.assertEqual(result["status"], "PENDING")

    def test_scheduler_failures_without_logs(self):
        spec = self.spec()
        self.store.add(spec, initial_runtime(spec))
        for state in ("TIMEOUT", "OUT_OF_MEMORY", "CANCELLED", "NODE_FAIL", "FAILED"):
            with self.subTest(state=state):
                self.assertEqual(self.do_poll(self.scheduler(state))["jobs"][0]["status"], state)

    def test_completed_not_success_without_marker(self):
        self.register(" SCF Done: E = -1\n")
        self.assertEqual(self.do_poll()["jobs"][0]["status"], "INSUFFICIENT")

    def test_old_log_mtime(self):
        spec = self.register()
        os.utime(spec["log"], (self.now - 500, self.now - 500))
        result = self.do_poll()["jobs"][0]
        self.assertEqual(result["status"], "INSUFFICIENT")
        self.assertIn("旧日志", result["reason"])

    def test_baseline_old_markers_with_append(self):
        spec = self.register(adopt=False)
        with Path(spec["log"]).open("a") as handle:
            handle.write("New run output without termination\n")
        self.assertEqual(self.do_poll()["jobs"][0]["status"], "INSUFFICIENT")

    def test_mixed_directory_only_exact_log(self):
        self.register("Unfinished\n")
        (self.root / "old.log").write_text(G16_SP)
        self.assertEqual(self.do_poll()["jobs"][0]["status"], "INSUFFICIENT")

    def test_original_file_bytes_unchanged(self):
        self.register()
        before = {p.name: p.read_bytes() for p in self.root.glob("*.log")}
        self.do_poll()
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.glob("*.log")})

    def test_dry_run_does_not_consume_or_enqueue(self):
        self.register()
        before = self.store.jobs("gaussian_user")
        self.assertEqual(self.do_poll(dry=True)["jobs"][0]["status"], "SUCCEEDED")
        self.assertEqual(self.store.jobs("gaussian_user"), before)
        self.assertEqual(self.store.outbox("gaussian_user"), [])

    def test_account_isolation_and_program_restriction(self):
        spec_a = self.register()
        spec_b = self.register(account="hpc_user")
        self.do_poll()
        self.assertEqual(self.store.jobs("hpc_user")[0]["result"], None)
        self.assertEqual(self.store.outbox("hpc_user"), [])
        self.assertTrue(spec_a["log"].endswith("1.log"))
        self.assertTrue(spec_b["log"].endswith("slurm-123.out"))
        with self.assertRaises(ValueError):
            self.spec(program="vasp", task="relax")

    def test_requeue_invalidates_evidence(self):
        self.register()
        self.do_poll(self.scheduler("RUNNING"))
        self.do_poll(self.scheduler("RUNNING", start=self.now - 10))
        result = self.do_poll(self.scheduler(start=self.now - 10))["jobs"][0]
        self.assertEqual(result["status"], "INSUFFICIENT")
        self.assertIn("重排队", result["reason"])

    def test_job_id_reuse_and_mismatched_workdir(self):
        self.register()
        self.do_poll(self.scheduler("RUNNING"))
        self.assertEqual(self.do_poll(self.scheduler(submit=self.now - 5))["jobs"][0]["status"], "INSUFFICIENT")
        self.assertEqual(self.do_poll(self.scheduler(workdir=str(self.root / "other")))["jobs"][0]["status"], "INSUFFICIENT")

    def test_steps_and_derived_exit(self):
        self.register()
        self.assertEqual(self.do_poll(self.scheduler(derived_exit_code="1:0"))["jobs"][0]["status"], "SLURM_FAILED")
        self.assertEqual(self.do_poll(self.scheduler(step_failures=[{"state": "OUT_OF_MEMORY"}]))["jobs"][0]["status"], "SLURM_FAILED")

    def test_incomplete_scheduler_or_query_failure(self):
        self.register()
        for patch_data in ({"start": None}, {"exit_code": None}, {"submit": None}, {"query_errors": ["squeue unavailable"]}):
            with self.subTest(data=patch_data):
                self.assertEqual(self.do_poll(self.scheduler(**patch_data))["jobs"][0]["status"], "INSUFFICIENT")

    def test_unknown_delay_and_recovery(self):
        self.register()
        record = self.scheduler("UNKNOWN")
        self.do_poll(record)
        self.assertEqual(len(self.store.outbox("gaussian_user")), 0)
        self.do_poll(record, now=self.now + 20)
        self.assertEqual(len(self.store.outbox("gaussian_user")), 1)
        self.do_poll()
        self.assertEqual(len(self.store.outbox("gaussian_user")), 2)

    def test_flush_grace(self):
        spec = self.register()
        runtime, result = observe(spec, initial_runtime(spec), self.scheduler(), self.now, {**CONFIG, "settle_seconds": 120})
        self.assertEqual(result["status"], "FINALIZING")
        self.assertFalse(result["notify"])

    def test_incremental_limit_defers_notification(self):
        spec = self.register("padding\n" * 1000 + G16_SP)
        runtime = initial_runtime(spec)
        config = {**CONFIG, "read_bytes": 1024}
        runtime, result = observe(spec, runtime, self.scheduler(), self.now, config)
        self.assertEqual(result["status"], "FINALIZING")
        self.assertFalse(result["notify"])
        for _ in range(10):
            runtime, result = observe(spec, runtime, self.scheduler(), self.now, config)
        self.assertEqual(result["status"], "SUCCEEDED")

    def test_batch_summary_failure_list(self):
        self.register()
        self.register(jid="124", text=G16_ERROR, log="second.log")
        result = self.do_poll()
        self.assertEqual(result["batches"]["demo"]["total"], 2)
        self.assertEqual(result["batches"]["demo"]["attention"][0]["job_id"], "124")

    def test_shared_log_mapping_rejected(self):
        self.register()
        with self.assertRaises(ValueError):
            spec = self.spec(jid="124")
            self.store.add(spec, initial_runtime(spec))

    def test_exclusive_lock(self):
        another = Store(self.root / "state.sqlite3")
        try:
            with self.store.lock():
                with self.assertRaises(OSError):
                    with another.lock():
                        self.fail("concurrent write allowed")
        finally:
            another.close()

    def test_truncation_and_same_size_overwrite(self):
        path = self.root / "file.log"
        path.write_text("old complete\n")
        _, cursor, _ = read_new(path, snapshot(path, adopt=True))
        path.write_text("new complete\n")
        lines, cursor, meta = read_new(path, cursor)
        self.assertTrue(meta["reset"])
        self.assertEqual(lines, ["new complete"])
        path.write_text("short\n")
        self.assertTrue(read_new(path, cursor)[2]["reset"])

    def test_partial_line_is_preserved(self):
        path = self.root / "file.log"
        path.write_bytes(b"Normal term")
        lines, cursor, meta = read_new(path, snapshot(path, adopt=True))
        self.assertEqual(lines, [])
        self.assertTrue(meta["partial_line"])
        with path.open("ab") as handle:
            handle.write(b"ination\n")
        self.assertEqual(read_new(path, cursor)[0], ["Normal termination"])


class SlurmTests(unittest.TestCase):
    def test_parent_not_step_and_array_mapping(self):
        text = "123_4|COMPLETED|0:0|0:0|2026-10-11T10:00:00|2026-10-11T10:01:00|2026-10-11T11:00:00|hpc_user|/calc\n123_4.batch|OUT_OF_MEMORY|0:9|0:9|2026-10-11T10:00:00|2026-10-11T10:01:00|2026-10-11T11:00:00|hpc_user|/calc\n"
        rows = parse_sacct(text)
        record = reconcile("123_4", "hpc_user", rows, [])
        self.assertEqual(record["state"], "COMPLETED")
        self.assertEqual(record["step_failures"][0]["state"], "OUT_OF_MEMORY")
        self.assertEqual(reconcile("123", "hpc_user", rows, [])["state"], "UNKNOWN")

    def test_queue_overrides_stale_accounting(self):
        old = parse_sacct("123|COMPLETED|0:0|0:0|2026-10-11T10:00:00|2026-10-11T10:01:00|2026-10-11T11:00:00|hpc_user|/calc\n")
        queue = parse_squeue("123|RUNNING|2026-10-11T10:00:00|2026-10-11T12:00:00|hpc_user|/calc\n")
        self.assertEqual(reconcile("123", "hpc_user", old, queue)["state"], "RUNNING")

    def test_cancelled_suffix_and_user_filter(self):
        rows = parse_sacct("123|CANCELLED by 1234|0:15|0:0|Unknown|Unknown|Unknown|hpc_user|/calc\n")
        self.assertEqual(rows[0]["state"], "CANCELLED")
        record = reconcile("123", "gaussian_user", rows, [])
        self.assertEqual(record["state"], "UNKNOWN")
        self.assertTrue(record["account_mismatch"])

    def test_query_failure_never_invents_terminal_state(self):
        calls = []
        def runner(args):
            calls.append(args)
            raise RuntimeError("unavailable")
        record = Slurm(runner).query("hpc_user", ["123"])["123"]
        self.assertEqual(record["state"], "UNKNOWN")
        self.assertEqual(len(record["query_errors"]), 2)
        self.assertTrue(all("hpc_user" in args for args in calls))

    def test_empty_workdir_and_latest_duplicate(self):
        rows = parse_sacct("123|FAILED|1:0|1:0|2026-10-11T10:00:00|2026-10-11T10:01:00|2026-10-11T11:00:00|hpc_user|\n123|RUNNING|0:0|0:0|2026-10-11T10:00:00|2026-10-11T12:00:00|Unknown|hpc_user|\n")
        self.assertEqual(len(rows), 2)
        self.assertEqual(reconcile("123", "hpc_user", rows, [])["state"], "RUNNING")


class MailTests(TempTests):
    def queued(self):
        self.register()
        self.do_poll()
        return self.store.outbox("gaussian_user")[0]

    def test_accepted_no_repeat(self):
        self.queued()
        sent = []
        sender = lambda row, settings: sent.append(row["id"])
        deliver(self.store, "gaussian_user", {"enabled": True}, self.now, sender)
        deliver(self.store, "gaussian_user", {"enabled": True}, self.now + 1, sender)
        self.assertEqual(len(sent), 1)
        self.assertEqual(self.store.outbox("gaussian_user")[0]["status"], "accepted")

    def test_failed_backoff_then_retry(self):
        self.queued()
        def failed(row, settings):
            raise DeliveryError("failed", "refused")
        deliver(self.store, "gaussian_user", {"enabled": True}, self.now, failed)
        self.assertEqual(deliver(self.store, "gaussian_user", {"enabled": True}, self.now + 1, failed), [])
        deliver(self.store, "gaussian_user", {"enabled": True}, self.now + 60, lambda row, settings: None)
        self.assertEqual(self.store.outbox("gaussian_user")[0]["status"], "accepted")

    def test_unknown_not_auto_retried(self):
        self.queued()
        def unknown(row, settings):
            raise DeliveryError("unknown", "connection lost during DATA")
        deliver(self.store, "gaussian_user", {"enabled": True}, self.now, unknown)
        self.assertEqual(deliver(self.store, "gaussian_user", {"enabled": True}, self.now + 1000, unknown), [])
        self.assertEqual(self.store.outbox("gaussian_user")[0]["status"], "unknown")

    def test_crash_during_sending_recovered_unknown(self):
        row = self.queued()
        self.store.mail_state(row["id"], "sending", self.now)
        deliver(self.store, "gaussian_user", {"enabled": True}, self.now + 10, lambda r, s: self.fail("must not resend"))
        self.assertEqual(self.store.outbox("gaussian_user")[0]["status"], "unknown")

    def test_smtp_phase_errors_and_quit(self):
        row = self.queued()
        class FakeSMTP:
            mode = "ok"
            def __init__(self, *args, **kwargs):
                self.asserted_context = kwargs["context"]
            def login(self, sender, password):
                if self.mode == "login":
                    raise OSError("credentials should never be logged")
            def send_message(self, *args, **kwargs):
                if self.mode == "data":
                    raise OSError("lost response")
                return {}
            def quit(self):
                raise OSError("quit failed after DATA success")
            def close(self):
                pass
        env = {"ATOMHPC_SMTP_SENDER": "test@example.com", "ATOMHPC_SMTP_PASSWORD": "secret", "ATOMHPC_SMTP_RECEIVER": "receiver@example.com"}
        with patch.dict(os.environ, env):
            self.assertEqual(send(row, {}, FakeSMTP), "accepted")
            for mode, expected in (("login", "failed"), ("data", "unknown")):
                FakeSMTP.mode = mode
                with self.assertRaises(DeliveryError) as caught:
                    send(row, {}, FakeSMTP)
                self.assertEqual(caught.exception.status, expected)
                self.assertNotIn("secret", str(caught.exception))


class CLITests(TempTests):
    def test_cli_register_status_and_atomic_import(self):
        config = {**CONFIG, "state_dir": str(self.root / "cli-state")}
        path = self.root / "config.json"
        path.write_text(json.dumps(config))
        base = ["--config", str(path), "--account", "gaussian_user"]
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(base + ["add", "--job-id", "123", "--program", "g16", "--task", "sp", "--workdir", str(self.root)]), 0)
            manifest = self.root / "jobs.json"
            manifest.write_text(json.dumps([{ "job_id": jid, "program": "g16", "task": "sp", "workdir": str(self.root), "log": jid + ".log"} for jid in ("124", "123")]))
            self.assertEqual(main(base + ["import-jobs", str(manifest)]), 2)
            self.assertEqual(main(base + ["status"]), 0)
        another = Store(self.root / "cli-state" / "gaussian_user.sqlite3")
        try:
            self.assertEqual(len(another.jobs("gaussian_user")), 1)
        finally:
            another.close()

    def test_live_poll_requires_account(self):
        path = self.root / "config.json"
        path.write_text(json.dumps({**CONFIG, "state_dir": str(self.root / "cli-state")}))
        with patch("atomhpcmonitor.cli.current_user", return_value="other"), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(["--config", str(path), "--account", "gaussian_user", "poll"]), 2)

    def test_rebind_preserves_history_and_resets_evidence(self):
        path = self.root / "config.json"
        path.write_text(json.dumps({**CONFIG, "state_dir": str(self.root)}))
        spec = self.register()
        self.do_poll()
        # CLI uses the account-specific database path.
        self.store.close()
        (self.root / "state.sqlite3").rename(self.root / "gaussian_user.sqlite3")
        self.store = Store(self.root / "gaussian_user.sqlite3")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--config", str(path), "--account", "gaussian_user", "rebind", "--job-id", "123", "--adopt-existing"]), 0)
        self.assertEqual(len(self.store.outbox("gaussian_user")), 1)
        job = self.store.jobs("gaussian_user")[0]
        self.assertIsNone(job["runtime"]["run_start"])
        self.assertEqual(job["runtime"]["cursor"]["offset"], 0)
        self.assertIsNone(job["result"])


if __name__ == "__main__":
    unittest.main()
