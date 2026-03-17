#!/usr/bin/env python3
"""
NoVA Kids — main pipeline runner.

Usage:
    python scripts/run_pipeline.py [OPTIONS]

Options:
    --source ID     Run only this source (can be repeated).
    --dry-run       Normalize and validate but do not write published files.
    --verbose       Enable DEBUG logging.

The pipeline steps are:
  1. Load enabled sources from config/sources.yaml
  2. Run each scraper → raw dicts saved to data/raw/
  3. Normalize each raw dict → Event objects saved to data/normalized/
  4. Enrich events (tags, scores) — done inside normalize_record()
  5. Deduplicate
  6. Validate — abort if any errors are found (warnings are noted)
  7. Publish weekly JSON to data/published/events/
  8. Print summary report
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.schema import Event
from config.settings import NORMALIZED_DIR, SOURCES_FILE
from enrichment.dedupe import deduplicate
from enrichment.normalize import normalize_record
from enrichment.publish import publish_events
from enrichment.validate import validate_events
from scrapers.base import ScraperError
from scrapers.registry import SCRAPERS

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Source loading
# ---------------------------------------------------------------------------

def load_sources(filter_ids: list[str] | None = None) -> list[dict]:
    """Load enabled sources from sources.yaml, optionally filtered by ID."""
    raw = yaml.safe_load(SOURCES_FILE.read_text(encoding="utf-8"))
    sources = raw.get("sources", [])
    enabled = [s for s in sources if s.get("enabled", False)]
    if filter_ids:
        enabled = [s for s in enabled if s["id"] in filter_ids]
    return enabled


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def run_scrapers(sources: list[dict]) -> list[dict]:
    """Run enabled scrapers and return all raw records."""
    all_raw: list[dict] = []
    for source in sources:
        source_id = source["id"]
        if source_id not in SCRAPERS:
            logging.warning("No scraper registered for source '%s' — skipping.", source_id)
            continue
        try:
            scraper = SCRAPERS[source_id]()
            records = scraper.run()
            all_raw.extend(records)
            logging.info("  %-35s  %4d raw records", source["name"], len(records))
        except ScraperError as exc:
            logging.error("Scraper error for '%s': %s", source_id, exc)
    return all_raw


def normalize_all(raw_records: list[dict]) -> list[Event]:
    """Normalize raw dicts to Event objects, dropping invalid ones."""
    events: list[Event] = []
    failed = 0
    for raw in raw_records:
        event = normalize_record(raw)
        if event:
            events.append(event)
        else:
            failed += 1
    logging.info("Normalization: %d ok, %d failed.", len(events), failed)
    return events


def save_normalized(events: list[Event]) -> None:
    """Persist normalized events as JSON for debugging."""
    payload = [e.model_dump() for e in events]
    out = NORMALIZED_DIR / "events.json"
    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    logging.debug("Normalized events saved to %s", out)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the NoVA Kids family activities pipeline."
    )
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        metavar="SOURCE_ID",
        help="Run only this source (repeat to include multiple).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip writing published files.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)

    print("\n=== NoVA Kids Pipeline ===\n")
    started_at = datetime.now(tz=timezone.utc)

    # 1. Load sources
    sources = load_sources(filter_ids=args.sources)
    if not sources:
        logging.error("No enabled sources found. Check config/sources.yaml.")
        return 1
    print(f"Sources enabled: {len(sources)}")

    # 2. Scrape
    print("\n[1/5] Scraping...")
    raw_records = run_scrapers(sources)
    print(f"      Raw records fetched: {len(raw_records)}")

    # 3. Normalize + enrich
    print("\n[2/5] Normalizing and enriching...")
    events = normalize_all(raw_records)
    save_normalized(events)
    print(f"      Events normalized:   {len(events)}")

    # 4. Deduplicate
    print("\n[3/5] Deduplicating...")
    events = deduplicate(events)
    print(f"      Events after dedupe: {len(events)}")

    # 5. Validate
    print("\n[4/5] Validating...")
    report = validate_events(events)
    print(f"      {report.summary()}")
    if not report.is_clean():
        print("\n  ERRORS (publishing blocked):")
        for issue in report.errors:
            print(f"    [{issue.rule}] {issue.event_title}: {issue.message}")
        print("\nPipeline halted due to validation errors.")
        return 1
    if report.warnings:
        print("  Warnings:")
        for issue in report.warnings:
            print(f"    [{issue.rule}] {issue.event_title}: {issue.message}")

    # 6. Publish
    if args.dry_run:
        print("\n[5/5] Dry run — skipping publish.")
    else:
        print("\n[5/5] Publishing...")
        result = publish_events(events)
        print(f"      Written: {result.output_path.name}")
        print(f"      Index updated: {result.index_path.name}")

    # Summary
    elapsed = (datetime.now(tz=timezone.utc) - started_at).total_seconds()
    print(f"\n=== Done in {elapsed:.1f}s ===")
    print(f"    Events published: {len(events)}")
    print(f"    Sources:          {', '.join(s['name'] for s in sources)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
