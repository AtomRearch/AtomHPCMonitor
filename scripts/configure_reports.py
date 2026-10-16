"""Update report preferences while preserving SMTP, date range and state paths."""
import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from atomhpcmonitor.reports import validate_settings


def configure(path, times):
    path = Path(path).resolve()
    config = json.loads(path.read_text(encoding="utf-8-sig"))
    settings = dict(config.get("reports", {}))
    settings.update(enabled=True, only_on_change=True, times=times,
                    timezone="UTC+08:00", programs=["g16", "vasp"])
    validate_settings(settings)
    config["reports"] = settings
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
            temporary = handle.name
            json.dump(config, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.chmod(temporary, path.stat().st_mode & 0o777)
        os.replace(temporary, str(path))
        temporary = None
    finally:
        if temporary is not None:
            os.unlink(temporary)
    print(json.dumps({"config": str(path), "reports": settings}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.local.json")
    parser.add_argument("--times", nargs="+", default=["07:30", "13:00", "21:30"])
    args = parser.parse_args()
    configure(args.config, args.times)
