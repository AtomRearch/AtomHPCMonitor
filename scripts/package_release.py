"""Build a public source ZIP from an explicit, credential-free allowlist."""
import hashlib
import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from atomhpcmonitor import __version__

FILES = [ROOT / name for name in (
    "README.md", "README.zh-CN.md", "LICENSE", "CHANGELOG.md", "CONTRIBUTING.md",
    "SECURITY.md", "pyproject.toml", "config.example.json", "smtp.example.json",
    ".gitignore", ".gitattributes", ".github/workflows/tests.yml",
    "docs/configuration.md", "docs/email-preview.html",
    "examples/demo.py", "examples/jobs.gaussian-user.json", "examples/jobs.hpc-user.json",
    "scripts/configure_reports.py", "scripts/export_mail_diagnostics.py",
    "scripts/package_release.py", "scripts/poll.sh")]
FILES += sorted((ROOT / "atomhpcmonitor").glob("*.py"))
FILES += sorted((ROOT / "tests").glob("*.py"))


if __name__ == "__main__":
    target = ROOT / "release" / ("AtomHPCMonitor-" + __version__ + ".zip")
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(str(target), "w", zipfile.ZIP_DEFLATED) as archive:
        for path in FILES:
            if not path.is_file():
                raise FileNotFoundError(str(path.relative_to(ROOT)))
            relative = path.relative_to(ROOT).as_posix()
            archive.writestr("AtomHPCMonitor/" + relative, path.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8"))
    with zipfile.ZipFile(str(target)) as archive:
        if archive.testzip() is not None:
            raise RuntimeError("ZIP integrity check failed")
    result = {"path": str(target), "files": len(FILES), "bytes": target.stat().st_size,
              "sha256": hashlib.sha256(target.read_bytes()).hexdigest()}
    target.with_suffix(".manifest.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result))
