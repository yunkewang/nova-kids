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

    for src_file in files:
        dst_file = DST / src_file.name
        shutil.copy2(src_file, dst_file)
        print(f"[sync] {src_file.name}")

    print(f"[sync] {len(files)} file(s) → public/events/")
    return 0


if __name__ == "__main__":
    sys.exit(sync())
