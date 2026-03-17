#!/usr/bin/env python3
"""
NoVA Kids — main pipeline runner.

Usage:
    python scripts/run_pipeline.py [OPTIONS]

Options:
    --source ID              Run only this source (can be repeated).
    --with-seed-discovery    Also include events resolved from seed discovery
                             (reads data/normalized/seed_events.json).
    --dry-run                Normalize and validate but do not write published files.
    --verbose                Enable DEBUG logging.

The pipeline steps are:
  1. Load enabled sources from config/sources.yaml
  2. Run each scraper → raw dicts saved to data/raw/
  3. (Optional) Load seed-resolved raws from data/normalized/seed_events.json
  4. Normalize all raw dicts → Event objects saved to data/normalized/
  5. Enrich events (tags, scores) — done inside normalize_record()
  6. Deduplicate
  7. Validate — abort if any errors are found (warnings are noted)
  8. Publish weekly JSON to data/published/events/
  9. Print summary report

To run seed discovery first:
    python scripts/run_seed_discovery.py
    python scripts/run_pipeline.py --with-seed-discovery
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.schema import Event
from config.settings import MANUAL_REVIEW_DIR, NORMALIZED_DIR, SOURCES_FILE
from enrichment.normalize import normalize_record
from enrichment.dedupe import deduplicate
from enrichment.publish import publish_events
from enrichment.validate import validate_events
from models.candidate import CandidateEvent, CandidateStatus
from scrapers.base import ScraperError
from scrapers.registry import SCRAPERS
from seed_discovery.dullesmoms_seed_finder import DullesMomsSeedFinder
from seed_discovery.resolver import resolve_candidates

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
# Manual review queue helpers (shared with run_seed_discovery.py logic)
# ---------------------------------------------------------------------------

def _load_review_queue() -> list[dict]:
    path = MANUAL_REVIEW_DIR / "pending_candidates.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logging.warning("Could not load review queue: %s", exc)
        return []


def _save_review_queue(
    new_candidates: list[CandidateEvent],
    dry_run: bool = False,
) -> None:
    """Merge new_candidates into pending_candidates.json (deduplicated by candidate_id)."""
    existing = {r["candidate_id"]: r for r in _load_review_queue()}
    for c in new_candidates:
        cid = c.candidate_id
        if cid not in existing:
            existing[cid] = c.model_dump()
        else:
            # Update automated fields; preserve any human-added notes
            existing[cid].update({
                k: v for k, v in c.model_dump().items()
                if k not in ("notes",) or not existing[cid].get("notes")
            })
    queue = list(existing.values())
    path = MANUAL_REVIEW_DIR / "pending_candidates.json"
    if not dry_run:
        path.write_text(json.dumps(queue, indent=2, default=str), encoding="utf-8")
        logging.info("Manual review queue: %d candidates → %s", len(queue), path)


def _load_candidates_for_week(target_week: date | None) -> list[CandidateEvent]:
    """Load pending/manual_review candidates, optionally filtered to target_week."""
    from dateutil import parser as dateutil_parser
    from datetime import timedelta

    raw_queue = _load_review_queue()
    candidates: list[CandidateEvent] = []
    for raw in raw_queue:
        try:
            c = CandidateEvent(**raw)
        except Exception as exc:
            logging.debug("Could not deserialize candidate %s: %s", raw.get("candidate_id"), exc)
            continue
        # Skip already published or rejected
        if c.status.value in ("published", "rejected"):
            continue
        # Apply week filter
        if target_week is not None and c.discovered_date_text:
            try:
                dt = dateutil_parser.parse(c.discovered_date_text, fuzzy=True)
                week_end = target_week + timedelta(days=6)
                if not (target_week <= dt.date() <= week_end):
                    continue
            except Exception:
                pass  # can't parse date → include
        candidates.append(c)
    return candidates


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
        "--week-start",
        metavar="DATE",
        default=None,
        help=(
            "Target week start date (YYYY-MM-DD, Monday). Used with "
            "--use-dullesmoms-seeds and --reprocess-existing-candidates to "
            "filter candidates and set the published week filename."
        ),
    )
    parser.add_argument(
        "--use-dullesmoms-seeds",
        action="store_true",
        dest="use_dullesmoms",
        help=(
            "Discover and resolve candidate events from DullesMoms inline, "
            "visiting each event detail page to find the original source URL. "
            "Use with --week-start to limit to a specific week."
        ),
    )
    parser.add_argument(
        "--reprocess-existing-candidates",
        action="store_true",
        dest="reprocess",
        help=(
            "Re-attempt resolution for candidates already in "
            "data/manual_review/pending_candidates.json. "
            "Use with --week-start to limit to a specific week."
        ),
    )
    parser.add_argument(
        "--with-seed-discovery",
        action="store_true",
        dest="with_seed",
        help=(
            "Include pre-resolved events from data/normalized/seed_events.json "
            "(produced by run_seed_discovery.py). Prefer --use-dullesmoms-seeds "
            "for a fully integrated run."
        ),
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

    # Parse --week-start
    target_week: date | None = None
    if args.week_start:
        try:
            target_week = date.fromisoformat(args.week_start)
        except ValueError:
            logging.error("Invalid --week-start: %r (expected YYYY-MM-DD)", args.week_start)
            return 1

    print("\n=== NoVA Kids Pipeline ===\n")
    if target_week:
        print(f"Target week: {target_week} (Mon)")
    started_at = datetime.now(tz=timezone.utc)

    # Counters for final summary
    n_discovered = n_resolved = n_duplicates_removed = n_manual_review = 0

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

    # 2b. DullesMoms inline seed discovery (new integrated mode)
    if args.use_dullesmoms:
        print(f"\n[seed] DullesMoms discovery (week={target_week or 'all'})...")
        finder = DullesMomsSeedFinder(target_week_start=target_week)
        try:
            candidates = finder.run()
        except Exception as exc:
            logging.error("DullesMoms seed finder failed: %s", exc)
            candidates = []
        n_discovered += len(candidates)
        print(f"       Candidates discovered: {len(candidates)}")

        if candidates:
            print(f"       Resolving {len(candidates)} candidates "
                  "(visiting detail pages as needed)...")
            seed_raws, manual_candidates = resolve_candidates(candidates)
            n_resolved += len(seed_raws)
            n_manual_review += len(manual_candidates)
            raw_records.extend(seed_raws)
            _save_review_queue(manual_candidates, dry_run=args.dry_run)
            print(f"       Resolved to publishable: {len(seed_raws)}")
            print(f"       Sent to manual review:   {len(manual_candidates)}")

    # 2c. Reprocess existing candidates from manual review queue
    if args.reprocess:
        candidates_to_retry = _load_candidates_for_week(target_week)
        if candidates_to_retry:
            print(f"\n[reprocess] Re-resolving {len(candidates_to_retry)} "
                  f"existing candidates (week={target_week or 'all'})...")
            n_discovered += len(candidates_to_retry)
            seed_raws, still_unresolved = resolve_candidates(candidates_to_retry)
            n_resolved += len(seed_raws)
            n_manual_review += len(still_unresolved)
            raw_records.extend(seed_raws)
            _save_review_queue(still_unresolved, dry_run=args.dry_run)
            print(f"       Newly resolved: {len(seed_raws)}")
            print(f"       Still unresolved: {len(still_unresolved)}")
        else:
            print("\n[reprocess] No matching candidates found in manual review queue.")

    # 2d. Load pre-resolved seed events from file (legacy --with-seed-discovery)
    if args.with_seed:
        seed_path = NORMALIZED_DIR / "seed_events.json"
        if seed_path.exists():
            seed_raws = json.loads(seed_path.read_text(encoding="utf-8"))
            raw_records.extend(seed_raws)
            print(f"      Seed records added (from file): {len(seed_raws)}")
        else:
            logging.warning(
                "--with-seed-discovery: %s not found. "
                "Run scripts/run_seed_discovery.py first.",
                seed_path,
            )

    # 3. Normalize + enrich
    print("\n[2/5] Normalizing and enriching...")
    events = normalize_all(raw_records)
    save_normalized(events)
    print(f"      Events normalized:   {len(events)}")

    # 3b. Filter to target week when specified
    if target_week is not None:
        from datetime import timedelta
        week_end = target_week + timedelta(days=6)
        before_filter = len(events)
        events = [e for e in events if target_week <= e.start.date() <= week_end]
        filtered_out = before_filter - len(events)
        if filtered_out:
            print(f"      Filtered to week {target_week}–{week_end}: "
                  f"{len(events)} kept, {filtered_out} outside range removed")

    # 4. Deduplicate
    print("\n[3/5] Deduplicating...")
    before_dedupe = len(events)
    events = deduplicate(events)
    n_duplicates_removed = before_dedupe - len(events)
    print(f"      Events after dedupe: {len(events)} ({n_duplicates_removed} duplicates removed)")

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
        result = publish_events(events, week_start=target_week)
        print(f"      Written: {result.output_path.name}")
        print(f"      Index updated: {result.index_path.name}")

    # Summary
    elapsed = (datetime.now(tz=timezone.utc) - started_at).total_seconds()
    print(f"\n=== Done in {elapsed:.1f}s ===")
    if args.use_dullesmoms or args.reprocess:
        print(f"    Discovered:         {n_discovered}")
        print(f"    Resolved:           {n_resolved}")
    print(f"    Published:          {len(events)}")
    if args.use_dullesmoms or args.reprocess:
        print(f"    Manual review:      {n_manual_review}")
    print(f"    Duplicates removed: {n_duplicates_removed}")
    print(f"    Sources:            {', '.join(s['name'] for s in sources)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
