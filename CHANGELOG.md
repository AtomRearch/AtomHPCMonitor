# Changelog

## 0.4.4

- Accept a single trailing parenthesized annotation in supported scalar INCAR parameters while retaining strict integer validation and unsupported-task checks.
- Publish generic configuration and synthetic examples, bilingual documentation, and an MIT license.

## 0.4.3

- Render scheduled report bodies from changed jobs only.
- Share per-job notification history across completion events, scheduled reports, and accepted manual reports.
- Avoid repeating unchanged discovery skips and old batch attention lists.
- Remove duplicated task metadata from HTML event notifications.
- Add a read-only mail diagnostics exporter; preserve unreported changes beyond the 100-job report limit.

## 0.4.2

- Emit compact continuous-monitor summaries; retain full observations with `run --json`.

## 0.4.1

- Add scheduled-report change detection that ignores timestamps and elapsed-time growth.
- Preserve report slot decisions and comparison history across restarts.

## 0.4.0

- Add Gaussian discovery and progress fields, bounded recent-job history, Slurm resource fields, and terminal snapshots.

## 0.3.x

- Add persistent HTML/plain-text mail, private SMTP JSON configuration, and Python 3.7 compatibility.

## 0.2.x

- Add VASP discovery and editable report date/time windows.

## 0.1.0

- Initial read-only monitoring, explicit job mappings, evidence reconciliation, SQLite events, and persistent mail outbox.
