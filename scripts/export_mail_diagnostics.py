"""Read-only export of recent mail evidence; no SMTP credentials or mail sending."""
import argparse
import json
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path


def export(config_path, account, output, limit=30):
    if not 1 <= limit <= 100:
        raise ValueError("limit must be 1..100")
    config_path = Path(config_path).resolve()
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    if account not in config.get("accounts", {}):
        raise ValueError("Account is not in configuration")
    database = (config_path.parent / config.get("state_dir", "runtime") / (account + ".sqlite3")).resolve()
    connection = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("BEGIN")
        mails = [dict(row) for row in connection.execute("""SELECT o.*, e.event_key,e.created,e.payload
            FROM outbox o JOIN events e ON e.id=o.event_id WHERE o.account=? ORDER BY o.id DESC LIMIT ?""", (account, limit))]
        for mail in mails:
            mail["payload"] = json.loads(mail["payload"])
        jobs = [dict(row) for row in connection.execute("SELECT job_id,result,enabled FROM jobs WHERE account=? ORDER BY job_id", (account,))]
        for job in jobs:
            job["result"] = json.loads(job["result"]) if job["result"] else None
        data = {"account": account, "exported_at_utc": datetime.now(timezone.utc).isoformat(),
                "database": str(database), "reports": config.get("reports", {}),
                "poll_seconds": config.get("poll_seconds", 300), "mails": mails, "jobs": jobs}
    finally:
        connection.close()
    log = config_path.parent / "logs" / ("monitor-" + account + ".log")
    tail = "No monitor log found."
    if log.is_file():
        with log.open("rb") as handle:
            handle.seek(0, 2)
            handle.seek(max(0, handle.tell() - 262144))
            tail = handle.read(262144).decode("utf-8", errors="replace")
    output = Path(output).resolve()
    if output.suffix.lower() != ".zip":
        raise ValueError("Output must be a .zip file")
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(str(output), "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("mail-evidence.json", json.dumps(data, ensure_ascii=False, indent=2))
        archive.writestr("monitor-tail.txt", tail)
    print(json.dumps({"output": str(output), "mail_count": len(mails), "read_only": True}, ensure_ascii=False))
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.local.json")
    parser.add_argument("--account", required=True)
    parser.add_argument("--output", help="Default: mail-diagnostics-<account>.zip")
    parser.add_argument("--limit", type=int, default=30)
    args = parser.parse_args()
    export(args.config, args.account, args.output or ("mail-diagnostics-" + args.account + ".zip"), args.limit)
