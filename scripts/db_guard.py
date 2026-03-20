#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def db_meta(path: Path):
    stat = path.stat()
    meta = {
        "path": str(path),
        "size_bytes": stat.st_size,
        "mtime_epoch": stat.st_mtime,
        "mtime_iso": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        "sha256": file_sha256(path),
    }
    try:
        conn = sqlite3.connect(str(path))
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        meta["table_count"] = len(row)
        conn.close()
    except sqlite3.Error:
        meta["table_count"] = None
    return meta


def snapshot_mode(db: Path, out: Path):
    if not db.exists():
        raise SystemExit(f"db not found: {db}")
    out.parent.mkdir(parents=True, exist_ok=True)
    meta = db_meta(db)
    with out.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print(f"snapshot_written={out}")
    print(json.dumps(meta, indent=2))


def verify_replace(current: Path, incoming: Path, force: bool):
    if not current.exists():
        raise SystemExit(f"current db not found: {current}")
    if not incoming.exists():
        raise SystemExit(f"incoming db not found: {incoming}")

    cur = db_meta(current)
    inc = db_meta(incoming)
    older = inc["mtime_epoch"] < cur["mtime_epoch"]
    smaller = inc["size_bytes"] < cur["size_bytes"]
    same_hash = inc["sha256"] == cur["sha256"]

    print("current:")
    print(json.dumps(cur, indent=2))
    print("incoming:")
    print(json.dumps(inc, indent=2))

    if same_hash:
        print("result=allowed (identical)")
        return
    if (older or smaller) and not force:
        reasons = []
        if older:
            reasons.append("older_mtime")
        if smaller:
            reasons.append("smaller_size")
        raise SystemExit(
            "result=blocked reasons="
            + ",".join(reasons)
            + " (use --force to override)"
        )
    print("result=allowed")


def backup_and_replace(current: Path, incoming: Path, backup_dir: Path):
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{current.name}.{stamp}.bak"
    shutil.copy2(current, backup_path)
    shutil.copy2(incoming, current)
    print(f"backup={backup_path}")
    print(f"replaced={current}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="DB overwrite guard using size/mtime/hash checks."
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    s = sub.add_parser("snapshot", help="Write metadata snapshot for a DB.")
    s.add_argument("--db", default="reminders.db")
    s.add_argument(
        "--out",
        default="outputs/db_guard/reminders_snapshot.json",
    )

    v = sub.add_parser("verify", help="Verify incoming DB is safe to replace current DB.")
    v.add_argument("--current", default="reminders.db")
    v.add_argument("--incoming", required=True)
    v.add_argument("--force", action="store_true")

    r = sub.add_parser("replace", help="Verify then backup + replace DB.")
    r.add_argument("--current", default="reminders.db")
    r.add_argument("--incoming", required=True)
    r.add_argument("--backup-dir", default="outputs/db_backups")
    r.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.mode == "snapshot":
        snapshot_mode(Path(args.db).expanduser().resolve(), Path(args.out).expanduser().resolve())
        return
    if args.mode == "verify":
        verify_replace(
            Path(args.current).expanduser().resolve(),
            Path(args.incoming).expanduser().resolve(),
            args.force,
        )
        return
    current = Path(args.current).expanduser().resolve()
    incoming = Path(args.incoming).expanduser().resolve()
    verify_replace(current, incoming, args.force)
    backup_and_replace(current, incoming, Path(args.backup_dir).expanduser().resolve())


if __name__ == "__main__":
    main()
