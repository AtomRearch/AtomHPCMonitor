# Security and private data

Keep `config.local.json`, `smtp.local.json`, environment files, state databases, monitor logs, real calculation outputs, and diagnostic archives private. The repository's ignore rules cover common local artifacts; use `git diff --cached` to review what will be published.

Use an SMTP app password where your provider requires one. The sender, receiver, and password can be loaded from a private JSON file or environment variables; they do not belong in issues, screenshots, or commits. Example addresses in this repository use reserved example domains.

`export_mail_diagnostics.py` excludes SMTP configuration and credentials, but its output includes job paths, saved mail bodies, and excerpts from calculation logs. Review and redact those details before sharing a diagnostic archive.

The monitor reads Slurm and calculation logs. It does not execute job submission scripts or modify calculation inputs, checkpoints, or scheduler jobs. Store state on a reliable filesystem with supported SQLite/file-lock semantics, and run one monitor per account/state database.

For a suspected security issue, describe the affected behavior without publishing live credentials or private calculation data. Rotate any credential that has already been disclosed; deleting a visible file alone does not remove it from Git history.
