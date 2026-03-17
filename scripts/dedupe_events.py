#!/usr/bin/env python3
"""
Standalone deduplication tool.

Reads events from a JSON file, deduplicates them, and writes the result.

Usage:
    python scripts/dedupe_events.py [INPUT] [--output OUTPUT]

INPUT defaults to data/normalized/events.json.
OUTPUT defaults to INPUT (in-place overwrite after confirmation).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.schema import Event
from config.settings import NORMALIZED_DIR
from enrichment.dedupe import deduplicate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Deduplicate NoVA Kids events JSON.")
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=NORMALIZED_DIR / "events.json",
        help="Path to input JSON file (list of events).",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Path to write deduplicated events. Defaults to INPUT (in-place).",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-8s %(message)s",
    )

    input_path: Path = args.input
    output_path: Path = args.output or input_path

    if not input_path.exists():
        print(f"File not found: {input_path}", file=sys.stderr)
        return 1

    raw_list = json.loads(input_path.read_text(encoding="utf-8"))
    if isinstance(raw_list, dict) and "events" in raw_list:
        raw_list = raw_list["events"]

    print(f"Loaded {len(raw_list)} records from {input_path}")

    events: list[Event] = []
    for item in raw_list:
        try:
            events.append(Event(**item))
        except Exception as exc:
            logging.warning("Skipping unparseable record: %s", exc)

    deduped = deduplicate(events)
    removed = len(events) - len(deduped)

    print(f"After dedup: {len(deduped)} events ({removed} removed)")

    if output_path == input_path:
        confirm = input(f"Overwrite {output_path}? [y/N] ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return 0

    payload = [e.model_dump() for e in deduped]
    output_path.write_text(
        json.dumps(payload, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"Written to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
