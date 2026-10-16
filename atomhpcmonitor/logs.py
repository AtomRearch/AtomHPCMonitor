"""Read only complete new lines, with append checks and bounded work per poll."""
import hashlib
from pathlib import Path


def snapshot(path, adopt=False):
    path = Path(path)
    try:
        with path.open("rb") as handle:
            stat = path.stat()
            offset = 0 if adopt else stat.st_size
            anchor = _anchor(handle, offset)
        return {"offset": offset, "inode": [stat.st_dev, stat.st_ino],
                "anchor": anchor, "pending": "", "baseline": offset, "lines": 0}
    except FileNotFoundError:
        return {"offset": 0, "inode": None, "anchor": None, "pending": "",
                "baseline": 0, "lines": 0}


def _anchor(handle, offset):
    handle.seek(max(0, offset - 256))
    return hashlib.sha256(handle.read(min(offset, 256))).hexdigest()


def read_new(path, cursor, max_bytes=8 * 1024 * 1024):
    """Returns lines, cursor, read metadata. Does not write the computation file."""
    cursor = dict(cursor)
    with Path(path).open("rb") as handle:
        before = Path(path).stat()
        reset = (cursor["inode"] not in (None, [before.st_dev, before.st_ino])
                 or before.st_size < cursor["offset"]
                 or (cursor["anchor"] is not None
                     and _anchor(handle, cursor["offset"]) != cursor["anchor"]))
        if reset:
            cursor = {"offset": 0, "inode": None, "anchor": None,
                      "pending": "", "baseline": 0, "lines": 0}
        start = cursor["offset"]
        handle.seek(start)
        raw = handle.read(min(max_bytes, before.st_size - start))
        cursor["offset"] += len(raw)
        cursor["inode"] = [before.st_dev, before.st_ino]
        # Latin-1 is a byte-preserving encoding; program markers are ASCII.
        combined = cursor["pending"] + raw.decode("latin-1")
        parts = combined.split("\n")
        cursor["pending"] = parts.pop()
        if len(cursor["pending"]) > 1024 * 1024:
            raise ValueError("日志单行超过 1 MiB，拒绝继续解析")
        lines = [part.rstrip("\r") for part in parts]
        cursor["lines"] += len(lines)
        cursor["anchor"] = _anchor(handle, cursor["offset"])
        after = Path(path).stat()
        stable = (before.st_size == after.st_size and before.st_mtime_ns == after.st_mtime_ns
                  and before.st_ino == after.st_ino)
        return lines, cursor, {"reset": reset, "stable": stable,
                               "caught_up": cursor["offset"] >= after.st_size,
                               "partial_line": bool(cursor["pending"]),
                               "mtime": after.st_mtime, "size": after.st_size}
