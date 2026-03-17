#!/usr/bin/env python3
"""
Validate events in a published or normalized JSON file.

Usage:
    python scripts/validate_events.py [FILE]

FILE defaults to data/normalized/events.json if not provided.
Exits with code 1 if any validation errors are found.
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
from enrichment.validate import validate_events


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate NoVA Kids event JSON files.")
    parser.add_argument(
        "file",
        nargs="?",
        type=Path,
        default=NORMALIZED_DIR / "events.json",
        help="Path to a JSON file containing a list of event objects.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-8s %(message)s",
    )

    path: Path = args.file
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        return 1

    raw_list = json.loads(path.read_text(encoding="utf-8"))

    # Support both bare list and weekly-file envelope
    if isinstance(raw_list, dict) and "events" in raw_list:
        raw_list = raw_list["events"]

    if not isinstance(raw_list, list):
        print("File must contain a JSON array of events.", file=sys.stderr)
        return 1

    print(f"Loading {len(raw_list)} events from {path}…")
    events: list[Event] = []
    parse_errors = 0
    for item in raw_list:
        try:
            events.append(Event(**item))
        except Exception as exc:
            parse_errors += 1
            logging.warning("Could not parse event: %s  (%s)", item.get("title"), exc)

    if parse_errors:
        print(f"  {parse_errors} records could not be parsed as Event objects.")

    report = validate_events(events)
    print(f"\n{report.summary()}")

    if report.errors:
        print("\nErrors:")
        for issue in report.errors:
            print(f"  [{issue.rule}] {issue.event_title} — {issue.message}")

    if report.warnings:
        print("\nWarnings:")
        for issue in report.warnings:
            print(f"  [{issue.rule}] {issue.event_title} — {issue.message}")

    return 0 if report.is_clean() else 1


if __name__ == "__main__":
    sys.exit(main())
