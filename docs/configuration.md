# Configuration and command reference

[English overview](../README.md) · [中文说明](../README.zh-CN.md)

## Configuration files

`--config` selects a JSON file. `state_dir` and `smtp.credentials_file` resolve relative to that configuration file. Start from [config.example.json](../config.example.json); it has email and scheduled reports disabled.

The `accounts` keys are actual Slurm/operating-system usernames. The supplied `hpc_user` and `gaussian_user` are placeholders. Update the account lists in `auto_discovery` and `reports` as well as the profile keys. Each login session monitors its own account and has a separate database.

| Setting | Default / role |
|---|---|
| `state_dir` | `runtime`, with one `<account>.sqlite3` file and OS lock per account. |
| `poll_seconds` | 300; minimum 30 seconds. |
| `settle_seconds` | 120; time for terminal records/logs to settle. |
| `unknown_seconds` | 600; delay before an unknown scheduler state becomes a notification event. |
| `read_bytes` | 8388608; maximum incremental bytes per log per poll. Large logs can require several polls. |
| `auto_discovery.program` | `all`, `g16`, or `vasp`; filtered by the account's supported programs. |
| `history.lookback_hours` | 24; configurable from 1 to 744 hours. |
| `history.max_jobs` | 100; configurable from 1 to 1000. |
| `reports.only_on_change` | `true`: send and display new or changed jobs only. `false` explicitly requests full periodic reports. |
| `reports.catch_up_minutes` | 60; a late scan can process a slot within its catch-up window. |

The date range includes both endpoints. Times must be zero-padded 24-hour `HH:MM`; use `00:00` for midnight. `timezone` is a fixed offset such as `UTC+08:00`, not an IANA zone with daylight-saving transitions. Restart `run` after editing configuration.

The convenience script below enables reports, sets the listed times and UTC+08:00, selects both programs, and preserves the existing account list, date range, SMTP settings, and state path:

```bash
python3 scripts/configure_reports.py --config config.local.json --times 07:30 13:00 21:30
```

## SMTP credentials

The private file referred to by `smtp.credentials_file` uses this schema:

```json
{
  "sender": "sender@example.com",
  "password": "REPLACE_WITH_SMTP_APP_PASSWORD",
  "receiver": "you@example.com"
}
```

Environment variables override private JSON values: `ATOMHPC_SMTP_SENDER`, `ATOMHPC_SMTP_PASSWORD`, and `ATOMHPC_SMTP_RECEIVER`. The corresponding environment variable names can also be configured. Set the actual provider host/port; the transport uses `smtplib.SMTP_SSL`, not STARTTLS.

Mail is persisted with the observation/event. Definite failures retain the original message and retry with backoff. An interrupted or ambiguous DATA exchange is `unknown`, requiring inbox review and an explicit operator retry; a successful DATA response remains `accepted` even if QUIT fails.

## Commands

All commands start with `python3 -m atomhpcmonitor --config config.local.json --account YOUR_SLURM_USER`.

| Command | Purpose |
|---|---|
| `doctor [--smtp]` | Check environment, account, Slurm access, and optionally SMTP login; no mail is sent. |
| `discover --program all [--register]` | Preview or register safely mapped jobs. |
| `poll --discover all [--dry-run]` | Run one monitoring cycle. |
| `run --discover all [--json]` | Continuous cycles; concise output by default. |
| `report --program all [--send] [--html-output preview.html]` | Preview a full overview or explicitly send that report. |
| `status [--batch NAME]` | Inspect saved jobs without contacting Slurm. |
| `events [--limit 20]` | Inspect saved decision evidence. |
| `outbox` | Inspect persisted delivery state and content. |
| `add` / `import-jobs FILE.json` | Register explicit job/log mappings. |
| `disable --job-id ID` | Stop polling a registered job, preserving history. |
| `rebind --job-id ID` | Re-establish a mapping after human review of restart/requeue identity. |
| `retry-mail ID` | Retry a failed message; unknown delivery additionally requires `--acknowledge-possible-duplicate`. |
| `deliver` | Deliver existing pending/retryable messages without rescanning jobs. |
| `test-mail` | Explicitly send one test message. |

`discover`, `poll`, `run`, and `report` accept `--history-hours N`. Live commands enforce the selected username. Saved-state commands are useful when inspecting a private copy of a database offline.

### Explicit mappings

For a calculation that does not fit auto-discovery, use an accurate job/log mapping:

```bash
python3 -m atomhpcmonitor --config config.local.json --account "$(id -un)" add --job-id 123456 --program g16 --task opt_freq --workdir /scratch/YOUR_SLURM_USER/molecule-A --log 1.log --expected-imaginary 0
```

By default, bytes already present at registration are baselined and ignored. Add `--adopt-existing` only after confirming that the existing log belongs to that job. Use `--expected-links N` for an explicitly mapped multi-Link1 calculation. Imaginary-frequency expectations and tolerance are operator-specified; they are not inferred scientific goals. Example JSON manifests are in [examples/](../examples/).

## Diagnostic exports

```bash
python3 scripts/export_mail_diagnostics.py --config config.local.json --account "$(id -un)"
```

This reads a consistent SQLite snapshot with `mode=ro` and exports recent mail, event payloads, job results, and up to 256 KiB from the monitor log. It does not migrate state or connect to SMTP. SMTP credentials/configuration are excluded, but the exported calculation evidence and paths remain private data. Review before sharing.

## Upgrades and recovery

Stop the old monitor on its original host, update source, retain `config.local.json`, `smtp.local.json`, and `runtime/`, then start one replacement. Terminal snapshots protect completed evidence when log paths are reused, while identity changes and requeues still require appropriate revalidation.

If a log is too large to read in one cycle, `caught_up: false` and `FINALIZING` can be expected during catch-up. A scheduler query failure, unreadable log, or missing timestamp is evidence of uncertainty and does not prove a program failure.
