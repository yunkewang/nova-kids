#!/usr/bin/env python3
"""
Sync published event JSON files into public/events/ for Vercel static hosting.

Usage:
    python scripts/sync_public.py

Copies every *.json file from data/published/events/ to public/events/.
Run this after any pipeline publish or repair before committing.
"""

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
SRC = ROOT / "data" / "published" / "events"
DST = ROOT / "public" / "events"


def sync() -> int:
    if not SRC.exists():
        print(f"[sync] Source not found: {SRC}", file=sys.stderr)
        return 1

    DST.mkdir(parents=True, exist_ok=True)

    files = sorted(SRC.glob("*.json"))
    if not files:
        print("[sync] No JSON files found in data/published/events/")
        return 0

    # Copy newer + updated files from source to destination.
    src_names = {f.name for f in files}
    for src_file in files:
        dst_file = DST / src_file.name
        shutil.copy2(src_file, dst_file)
        print(f"[sync] {src_file.name}")

    # Mirror deletions: remove any week-*.json in public/events/ that no
    # longer exists in data/published/events/. This keeps the Vercel mirror
    # in lockstep with the retention policy enforced in enrichment/publish.py.
    removed = 0
    for dst_file in sorted(DST.glob("week-*.json")):
        if dst_file.name not in src_names:
            try:
                dst_file.unlink()
                print(f"[sync] removed stale {dst_file.name}")
                removed += 1
            except OSError as exc:
                print(f"[sync] could not remove {dst_file}: {exc}", file=sys.stderr)

    tail = f" ({removed} removed)" if removed else ""
    print(f"[sync] {len(files)} file(s) → public/events/{tail}")
    return 0


if __name__ == "__main__":
    sys.exit(sync())
