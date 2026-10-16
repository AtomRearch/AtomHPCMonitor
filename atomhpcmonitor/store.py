"""Durable jobs, observations and mail outbox, isolated by account."""
import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from .slurm import settled_result


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                account TEXT NOT NULL, job_id TEXT NOT NULL, spec TEXT NOT NULL,
                runtime TEXT NOT NULL, result TEXT, enabled INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(account, job_id));
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY, event_key TEXT NOT NULL UNIQUE,
                account TEXT NOT NULL, job_id TEXT NOT NULL, batch TEXT NOT NULL,
                created REAL NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL UNIQUE,
                account TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0, next_try REAL NOT NULL DEFAULT 0,
                updated REAL NOT NULL, error TEXT, subject TEXT NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS report_slots (
                slot_key TEXT PRIMARY KEY, account TEXT NOT NULL,
                checked REAL NOT NULL, outcome TEXT NOT NULL);
        """)
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            if "html_body" not in {row["name"] for row in self.db.execute("PRAGMA table_info(outbox)")}:
                self.db.execute("ALTER TABLE outbox ADD COLUMN html_body TEXT")

    def close(self):
        self.db.close()

    @contextmanager
    def lock(self):
        """OS lock releases on crash; only one poller/deliverer per database."""
        handle = self.path.with_suffix(self.path.suffix + ".lock").open("a+b")
        try:
            if handle.seek(0, os.SEEK_END) == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield
            finally:
                handle.seek(0)
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def add(self, spec, runtime):
        self.register_many([spec], [runtime])

    def check_log_mapping(self, specs, exclude_job=None):
        occupied = {json.loads(row["spec"])["log"]: row["job_id"] for row in
                    self.db.execute("SELECT spec,job_id,result FROM jobs WHERE account=? AND enabled=1",
                                    (specs[0]["account"],)) if row["job_id"] != exclude_job
                    and not settled_result(json.loads(row["result"]) if row["result"] else None)}
        for spec in specs:
            if spec["log"] in occupied:
                raise ValueError(f"主日志已映射到 Job {occupied[spec['log']]}: {spec['log']}；每个活动作业需要独立日志")
            occupied[spec["log"]] = spec["job_id"]

    def register_many(self, specs, runtimes):
        self.check_log_mapping(specs)
        with self.db:
            for spec, runtime in zip(specs, runtimes):
                self.db.execute("INSERT INTO jobs(account,job_id,spec,runtime) VALUES(?,?,?,?)",
                                (spec["account"], spec["job_id"], encode(spec), encode(runtime)))

    def jobs(self, account, enabled_only=True):
        rows = self.db.execute("SELECT * FROM jobs WHERE account=?" +
                               (" AND enabled=1" if enabled_only else "") + " ORDER BY job_id", (account,))
        return [{"spec": json.loads(r["spec"]), "runtime": json.loads(r["runtime"]),
                 "result": json.loads(r["result"]) if r["result"] else None,
                 "enabled": bool(r["enabled"])} for r in rows]

    def save(self, spec, runtime, result, event, now):
        # Commit observation and outbox together: no completion lost between them.
        with self.db:
            self.db.execute("UPDATE jobs SET runtime=?,result=? WHERE account=? AND job_id=?",
                            (encode(runtime), encode(result), spec["account"], spec["job_id"]))
            if event:
                cursor = self.db.execute(
                    "INSERT OR IGNORE INTO events(event_key,account,job_id,batch,created,payload) VALUES(?,?,?,?,?,?)",
                    (event["key"], spec["account"], spec["job_id"], spec["batch"], now, encode(result)))
                if cursor.rowcount:
                    self.db.execute("INSERT INTO outbox(event_id,account,updated,subject,body,html_body) VALUES(?,?,?,?,?,?)",
                                    (cursor.lastrowid, spec["account"], now, event["subject"], event["body"], event.get("html_body")))

    def outbox(self, account):
        return [dict(r) for r in self.db.execute("SELECT * FROM outbox WHERE account=? ORDER BY id", (account,))]

    def enqueue_report(self, account, key, subject, body, payload, now, html_body=None):
        with self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO events(event_key,account,job_id,batch,created,payload) VALUES(?,?,?,?,?,?)",
                (key, account, "@report", "progress-report", now, encode(payload)))
            if not cursor.rowcount:
                return None
            outbox = self.db.execute("INSERT INTO outbox(event_id,account,updated,subject,body,html_body) VALUES(?,?,?,?,?,?)",
                                     (cursor.lastrowid, account, now, subject, body, html_body))
            return outbox.lastrowid

    def report_slot_seen(self, key):
        return self.db.execute("SELECT 1 FROM report_slots WHERE slot_key=?", (key,)).fetchone() is not None

    def mark_report_slot(self, account, key, now, outcome):
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO report_slots VALUES(?,?,?,?)", (key, account, now, outcome))

    def report_baseline(self, account, programs):
        # All automatic mail shares per-job snapshots, including completion mail.
        # Pending automatic mail owns its snapshot through retry/unknown delivery.
        rows = self.db.execute("""SELECT e.event_key,e.payload,o.status FROM events e
            JOIN outbox o ON o.event_id=e.id WHERE e.account=?
            ORDER BY e.id DESC""", (account,))
        jobs = {}
        found = False
        for row in rows:
            if row["status"] != "accepted" and row["event_key"].startswith("manual-report:"):
                continue
            payload = json.loads(row["payload"])
            if payload.get("type") == "progress-report":
                # Older versions stored all jobs but displayed only the first 100.
                candidates = payload.get("jobs", [])[:100]
            else:
                candidates = [payload] if payload.get("job_id") and payload.get("program") else []
            for job in candidates:
                scheduler = job.get("scheduler", {})
                if job.get("program") in programs and scheduler.get("state") not in (None, "UNKNOWN") and not scheduler.get("query_errors"):
                    found = True
                    jobs.setdefault(job["job_id"], job)
        # A temporary query outage in a later report must not erase an earlier
        # confirmed observation and turn simple recovery into a "new job".
        return {"jobs": list(jobs.values())} if found else None

    def reported_skips(self, account):
        result = {}
        rows = self.db.execute("""SELECT e.event_key,e.payload,o.status FROM events e
            JOIN outbox o ON o.event_id=e.id WHERE e.account=? AND e.batch='progress-report'
            ORDER BY e.id DESC""", (account,))
        for row in rows:
            if row["status"] != "accepted" and row["event_key"].startswith("manual-report:"):
                continue
            discovery = json.loads(row["payload"]).get("discovery") or {}
            for item in discovery.get("skipped", [])[:10]:
                result.setdefault(item["job_id"], item)
        return result

    def mail_state(self, message_id, status, now, error=None, next_try=0, increment=False):
        with self.db:
            self.db.execute("UPDATE outbox SET status=?,updated=?,error=?,next_try=?,attempts=attempts+? WHERE id=?",
                            (status, now, error, next_try, int(increment), message_id))

    def recover_mail(self, account, now):
        with self.db:
            self.db.execute("UPDATE outbox SET status='unknown',updated=?,error=? WHERE account=? AND status='sending'",
                            (now, "进程在 SMTP 投递期间中断；结果不明，需核验后手动重试", account))
