import contextlib
import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path
from scripts.export_mail_diagnostics import export


class MailDiagnosticTests(unittest.TestCase):
    def test_old_database_exports_without_migration_or_credentials(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            state = root / "state"
            state.mkdir()
            database = state / "hpc_user.sqlite3"
            db = sqlite3.connect(str(database))
            db.executescript('''CREATE TABLE events(id INTEGER PRIMARY KEY,event_key TEXT,created REAL,payload TEXT);
                CREATE TABLE outbox(id INTEGER PRIMARY KEY,event_id INTEGER,account TEXT,status TEXT,subject TEXT,body TEXT);
                CREATE TABLE jobs(job_id TEXT,account TEXT,result TEXT,enabled INTEGER);''')
            db.execute("INSERT INTO events VALUES(1,'report:one',1,?)", (json.dumps({"type": "progress-report", "jobs": []}),))
            db.execute("INSERT INTO outbox VALUES(1,1,'hpc_user','accepted','progress','mail body')")
            db.execute("INSERT INTO jobs VALUES('42','hpc_user',?,1)", (json.dumps({"status": "RUNNING"}),))
            db.commit()
            db.close()
            secret = "SMTP_SECRET_MUST_NOT_EXPORT"
            config = root / "config.local.json"
            config.write_text(json.dumps({"accounts": {"hpc_user": {}}, "state_dir": "state",
                "reports": {"times": ["07:30"]}, "smtp": {"password": secret, "credentials_file": "smtp.local.json"}}), encoding="utf-8")
            (root / "smtp.local.json").write_text(secret, encoding="utf-8")
            (root / "logs").mkdir()
            (root / "logs/monitor-hpc_user.log").write_bytes(b"past output\n" + b"a" * 300000 + b"\nlatest output\n")
            before = hashlib.sha256(database.read_bytes()).hexdigest()
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = export(config, "hpc_user", root / "diagnostic.zip")
            self.assertEqual(hashlib.sha256(database.read_bytes()).hexdigest(), before)
            with zipfile.ZipFile(str(result)) as archive:
                self.assertEqual(set(archive.namelist()), {"mail-evidence.json", "monitor-tail.txt"})
                self.assertTrue(all(secret.encode() not in archive.read(n) for n in archive.namelist()))
                evidence = json.loads(archive.read("mail-evidence.json"))
                self.assertEqual(evidence["mails"][0]["body"], "mail body")
                self.assertEqual(evidence["jobs"][0]["result"]["status"], "RUNNING")
                self.assertLessEqual(len(archive.read("monitor-tail.txt")), 262144)
                self.assertIn(b"latest output", archive.read("monitor-tail.txt"))
                self.assertNotIn("smtp", evidence)
            self.assertNotIn(secret, stdout.getvalue())
            db = sqlite3.connect(str(database))
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='report_slots'").fetchone())
            db.close()

    def test_missing_database_is_not_created(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = root / "config.json"
            config.write_text(json.dumps({"accounts": {"hpc_user": {}}}))
            with self.assertRaises(sqlite3.OperationalError):
                export(config, "hpc_user", root / "diagnostic.zip")
            self.assertFalse((root / "runtime").exists())
            self.assertFalse((root / "diagnostic.zip").exists())


if __name__ == "__main__":
    unittest.main()
