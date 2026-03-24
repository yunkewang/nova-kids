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
# Public sync — keeps public/events/ in step with data/published/events/
# ---------------------------------------------------------------------------

def _sync_public() -> None:
    """Copy all published JSON files into public/events/ for Vercel hosting."""
    import shutil
    src_dir = Path(__file__).parent.parent / "data" / "published" / "events"
    dst_dir = Path(__file__).parent.parent / "public" / "events"
    dst_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for src_file in sorted(src_dir.glob("*.json")):
        shutil.copy2(src_file, dst_dir / src_file.name)
        count += 1
    if count:
        print(f"[sync] {count} file(s) → public/events/")


# ---------------------------------------------------------------------------
# Family relevance filter
# ---------------------------------------------------------------------------

def _filter_for_family_feed(
    events: list[Event],
    dry_run: bool = False,
) -> tuple[list[Event], list[Event]]:
    """
    Split *events* into (publish_list, excluded_list) based on family relevance.

    Events whose family_relevance_score is below the publish threshold are
    moved to the excluded list and written to
    data/manual_review/excluded_non_family_events.json for audit.

    Safety: if filtering would remove >50% of events, the function logs a
    warning and returns all events unfiltered (to prevent an aggressive
    regex change from silently gutting the feed).

    Returns (to_publish, excluded).
    """
    from enrichment.family_relevance import PUBLISH_THRESHOLD
    from config.settings import MANUAL_REVIEW_DIR

    to_publish: list[Event] = []
    excluded: list[Event] = []

    for ev in events:
        if ev.family_relevance_score >= PUBLISH_THRESHOLD:
            to_publish.append(ev)
        else:
            excluded.append(ev)

    # Safety guard — bail out if filter is too aggressive
    if excluded and len(excluded) / len(events) > 0.50:
        print(
            f"\n  [WARN] Family relevance filter would remove {len(excluded)}/{len(events)} "
            f"events ({len(excluded)*100//len(events)}%).\n"
            f"  This exceeds the 50% safety threshold — filtering ABORTED to protect feed.\n"
            f"  Review family_relevance.py thresholds or run with --dry-run first."
        )
        return events, []

    # Write excluded events to manual review dir
    if excluded and not dry_run:
        report_path = MANUAL_REVIEW_DIR / "excluded_non_family_events.json"
        report_records = [
            {
                "id": ev.id,
                "title": ev.title,
                "source_name": ev.source_name,
                "source_url": ev.source_url,
                "start": ev.start.isoformat() if ev.start else None,
                "summary": ev.summary,
                "family_relevance_score": ev.family_relevance_score,
                "family_relevance_label": ev.family_relevance_label,
                "tags": ev.tags,
                "location_name": ev.location_name,
            }
            for ev in sorted(excluded, key=lambda e: e.family_relevance_score)
        ]
        report_path.write_text(
            json.dumps(report_records, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"  Exclusion report: {report_path.name} ({len(report_records)} events)")

    return to_publish, excluded


def _print_filter_summary(
    events_before: list[Event],
    excluded: list[Event],
) -> None:
    """Print a brief family-relevance filter report to stdout."""
    from collections import Counter
    from enrichment.family_relevance import PUBLISH_THRESHOLD

    if not excluded:
        print(f"  Family filter:    0 excluded (threshold={PUBLISH_THRESHOLD})")
        return

    print(f"\n  Family relevance filter (threshold={PUBLISH_THRESHOLD}):")
    print(f"    Candidate events:   {len(events_before)}")
    print(f"    Published events:   {len(events_before) - len(excluded)}")
    print(f"    Excluded events:    {len(excluded)}")

    # Top exclusion reasons (from family_relevance_label — stored on Event now)
    label_counts: Counter = Counter(ev.family_relevance_label for ev in excluded)
    print(f"    Exclusion labels:   {dict(label_counts)}")

    # Show up to 8 examples
    print(f"    Examples excluded:")
    for ev in sorted(excluded, key=lambda e: e.family_relevance_score)[:8]:
        print(f"      [{ev.family_relevance_score:.2f}] {ev.title[:60]!r}  ({ev.source_name})")


# ---------------------------------------------------------------------------
# Cost inference reporting helpers
# ---------------------------------------------------------------------------

def _print_cost_summary(events: list[Event]) -> None:
    """Print a one-line cost breakdown to stdout."""
    from collections import Counter
    counts: Counter = Counter(e.cost_type for e in events)
    total = len(events)
    free    = counts.get("free", 0)
    paid    = counts.get("paid", 0)
    donation = counts.get("suggested_donation", 0)
    unknown = counts.get("unknown", 0)
    print(f"\n  Cost breakdown ({total} events):")
    print(f"    Free:     {free:4d}  ({free / total * 100:.0f}%)" if total else "    Free:        0")
    print(f"    Paid:     {paid:4d}")
    print(f"    Donation: {donation:4d}")
    print(f"    Unknown:  {unknown:4d}")


def _print_cost_inference_detail(events: list[Event]) -> None:
    """Print per-reason cost inference breakdown."""
    from collections import Counter
    from enrichment.normalize import infer_cost

    reason_counts: Counter = Counter()
    for e in events:
        _, _, reason = infer_cost(
            e.price_text if e.cost_type not in ("free",) else None,
            e.summary,
            source_name=e.source_name,
            source_url=e.source_url,
            location_name=e.location_name,
            title=e.title,
        )
        reason_counts[reason] += 1

    print("\n  Cost inference reasons:")
    for reason, count in reason_counts.most_common():
        print(f"    {count:4d}  {reason}")


def _write_cost_inference_report(
    repaired: list[Event],
    old_cost: dict[str, str],
    week_start: date,
) -> None:
    """
    Write a JSON cost inference report to data/manual_review/cost_inference_report.json.

    Includes before/after comparison, per-reason counts, and examples of
    events that changed from unknown → free.
    """
    import json as _json
    from collections import Counter
    from enrichment.normalize import infer_cost
    from config.settings import MANUAL_REVIEW_DIR

    reason_counts: Counter = Counter()
    changed_to_free: list[dict] = []
    changed_to_paid: list[dict] = []

    for e in repaired:
        prev = old_cost.get(e.id, "unknown")
        curr = e.cost_type if isinstance(e.cost_type, str) else e.cost_type.value

        _, _, reason = infer_cost(
            None if curr == "free" else e.price_text,
            e.summary,
            source_name=e.source_name,
            source_url=e.source_url,
            location_name=e.location_name,
            title=e.title,
        )
        reason_counts[reason] += 1

        if prev != curr:
            entry = {
                "id": e.id,
                "title": e.title,
                "source_name": e.source_name,
                "location_name": e.location_name,
                "cost_before": prev,
                "cost_after": curr,
                "price_text": e.price_text,
                "reason": reason,
            }
            if prev != "free" and curr == "free":
                changed_to_free.append(entry)
            elif curr == "paid":
                changed_to_paid.append(entry)

    from collections import Counter as _Counter
    new_cost_counts: _Counter = _Counter(
        (e.cost_type if isinstance(e.cost_type, str) else e.cost_type.value)
        for e in repaired
    )

    report = {
        "week_start": week_start.isoformat(),
        "total_events": len(repaired),
        "cost_counts": dict(new_cost_counts),
        "newly_inferred_free": len(changed_to_free),
        "newly_inferred_paid": len(changed_to_paid),
        "top_inference_reasons": reason_counts.most_common(),
        "examples_changed_to_free": changed_to_free[:20],
        "examples_changed_to_paid": changed_to_paid[:10],
    }

    report_path = MANUAL_REVIEW_DIR / "cost_inference_report.json"
    report_path.write_text(_json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n[cost] Inference report written → {report_path}")
    print(f"[cost] Newly inferred free: {len(changed_to_free)}")
    print(f"[cost] Newly inferred paid: {len(changed_to_paid)}")
    print(f"[cost] Top inference reasons:")
    for reason, count in reason_counts.most_common(6):
        print(f"         {count:4d}  {reason}")
    if changed_to_free:
        print(f"\n[cost] Example events newly classified as free:")
        for ex in changed_to_free[:5]:
            print(f"         [{ex['reason']}] {ex['title']!r}  ({ex['source_name']})")


# ---------------------------------------------------------------------------
# Repair mode
# ---------------------------------------------------------------------------

def _repair_published_week(
    week_start: date,
    dry_run: bool,
    filter_non_family: bool = False,
    improve_cost: bool = False,
) -> int:
    """
    Re-enrich and re-publish a previously published week JSON.

    Reads the existing week-YYYY-MM-DD.json, re-runs normalize → dedupe →
    validate → publish so that enrichment improvements (venue hints, new tags,
    better short_notes) are applied without re-scraping.
    """
    from config.settings import PUBLISHED_DIR

    filename = f"week-{week_start.isoformat()}.json"
    week_path = PUBLISHED_DIR / filename
    if not week_path.exists():
        logging.error("Repair: %s not found.", week_path)
        return 1

    raw_payload = json.loads(week_path.read_text(encoding="utf-8"))
    existing_events: list[dict] = raw_payload.get("events", [])
    if not existing_events:
        logging.warning("Repair: no events found in %s.", filename)
        return 0

    print(f"[repair] Loaded {len(existing_events)} events from {filename}")

    # Snapshot old cost_type per event id (for before/after comparison)
    old_cost: dict[str, str] = {
        ev.get("id", ""): ev.get("cost_type", "unknown")
        for ev in existing_events
    }

    # Re-normalize each event (re-runs enrichment pipeline on the stored raw data)
    repaired: list[Event] = []
    failed = 0
    for ev in existing_events:
        # Convert stored event dict back to a raw scraper-style dict.
        # Pass original price_text (before any "Free" inference override)
        # so that infer_cost() can re-examine it with the new logic.
        original_price = ev.get("price_text")
        if original_price == "Free" and ev.get("cost_type") in ("free",):
            # If previously inferred free, clear so new logic re-evaluates cleanly
            original_price = None
        raw: dict = {
            "title": ev.get("title", ""),
            "start_text": ev.get("start", ""),
            "end_text": ev.get("end"),
            "location_text": " ".join(filter(None, [
                ev.get("location_name"), ev.get("location_address"),
            ])),
            "price_text": original_price,
            "summary_text": ev.get("summary"),
            "source_name": ev.get("source_name", ""),
            "source_url": ev.get("source_url", ""),
            "registration_url": ev.get("registration_url"),
            "image_url": ev.get("image_url"),
            "extracted_from": ev.get("extracted_from", "repaired"),
            "extraction_confidence": ev.get("extraction_confidence", 1.0),
            "all_day": ev.get("all_day", False),
        }
        event = normalize_record(raw)
        if event:
            repaired.append(event)
        else:
            failed += 1

    print(f"[repair] Re-normalized: {len(repaired)} ok, {failed} failed")

    repaired = deduplicate(repaired)
    print(f"[repair] After dedupe: {len(repaired)}")

    # Optional family-relevance filter
    if filter_non_family:
        before_filter = list(repaired)
        repaired, excluded = _filter_for_family_feed(repaired, dry_run=dry_run)
        _print_filter_summary(before_filter, excluded)

    report = validate_events(repaired)
    print(f"[repair] {report.summary()}")
    if not report.is_clean():
        for issue in report.errors:
            print(f"  ERROR [{issue.rule}] {issue.event_title}: {issue.message}")
        print("[repair] Halted due to validation errors.")
        return 1

    if dry_run:
        print("[repair] Dry run — skipping publish.")
    else:
        result = publish_events(repaired, week_start=week_start)
        print(f"[repair] Republished {result.event_count} events → {result.output_path.name}")
        _sync_public()

    # Cost summary always shown after repair
    _print_cost_summary(repaired)

    # Detailed cost inference report (--improve-cost-inference)
    if improve_cost:
        _write_cost_inference_report(repaired, old_cost, week_start)

    return 0


def _repair_geo_enrich(
    week_start: date,
    dry_run: bool,
    strict_region: bool = False,
) -> int:
    """
    Patch an already-published week JSON with lat/lon for events that lack
    coordinates. Does not re-normalize — loads and updates event dicts directly.

    strict_region=True additionally:
      - Nulls existing coordinates that fall outside the NoVA/DC service area.
      - Skips virtual events and nulls any coordinates they hold.
      - Retries geocoding for events whose coordinates were nulled.
      - Writes suspicious geocodes to data/manual_review/suspicious_geocodes.json.
      - Purges out-of-region entries from the geocode cache before running.
    """
    from config.settings import PUBLISHED_DIR, MANUAL_REVIEW_DIR
    from enrichment.geocode import geocode_event_dicts, load_cache

    filename = f"week-{week_start.isoformat()}.json"
    week_path = PUBLISHED_DIR / filename
    if not week_path.exists():
        logging.error("Geo-repair: %s not found.", week_path)
        return 1

    payload = json.loads(week_path.read_text(encoding="utf-8"))
    event_dicts: list[dict] = payload.get("events", [])
    if not event_dicts:
        logging.warning("Geo-repair: no events found in %s.", filename)
        return 0

    mode = "strict-region" if strict_region else "standard"
    print(f"[geo-repair] Loaded {len(event_dicts)} events from {filename} [{mode}]")

    cache = load_cache()
    print(f"[geo-repair] Cache: {cache.size} existing entries")

    if strict_region:
        purged = cache.invalidate_out_of_region()
        if purged:
            print(f"[geo-repair] Purged {purged} out-of-region cache entries")

    updated_dicts, stats, suspicious = geocode_event_dicts(
        event_dicts, cache, strict_region=strict_region
    )
    stats.print_summary()

    if suspicious:
        print(f"\n  Suspicious geocodes identified: {len(suspicious)}")
        for s in suspicious:
            print(f"    [{s['rejection_reason']}] {s['title']}")
        if not dry_run:
            suspicious_path = MANUAL_REVIEW_DIR / "suspicious_geocodes.json"
            suspicious_path.write_text(
                json.dumps(suspicious, indent=2), encoding="utf-8"
            )
            print(f"  Written to {suspicious_path.name}")

    if dry_run:
        print("\n[geo-repair] Dry run — skipping write.")
    else:
        payload["events"] = updated_dicts
        payload["generated_at"] = datetime.now(tz=timezone.utc).isoformat()
        week_path.write_text(
            json.dumps(payload, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"\n[geo-repair] Updated {filename}")
        _sync_public()

    return 0


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
        "--repair-published-week",
        nargs="?",
        const="",
        default=None,
        dest="repair_week",
        metavar="DATE",
        help=(
            "Re-enrich and re-publish an already-published week file. "
            "Accepts an explicit YYYY-MM-DD date or uses --week-start when "
            "no date is given. Re-runs normalize + dedupe + validate + publish. "
            "Combine with --enrich-geo to geocode missing coordinates only "
            "(faster — skips full re-normalization)."
        ),
    )
    parser.add_argument(
        "--enrich-geo",
        action="store_true",
        dest="enrich_geo",
        help=(
            "Geocode events that are missing latitude/longitude using Photon. "
            "In normal pipeline mode: runs after deduplicate. "
            "With --repair-published-week: patches the existing week JSON "
            "with coordinates without full re-normalization."
        ),
    )
    parser.add_argument(
        "--strict-region",
        action="store_true",
        dest="strict_region",
        help=(
            "Enforce NoVA/DC metro service-area validation. "
            "With --repair-published-week --enrich-geo: also nulls existing "
            "out-of-region coordinates, skips virtual events, and writes "
            "suspicious geocodes to data/manual_review/suspicious_geocodes.json."
        ),
    )
    parser.add_argument(
        "--filter-non-family",
        action="store_true",
        dest="filter_non_family",
        help=(
            "Exclude events below the family relevance threshold from the "
            "published feed. Adult-service events (tax prep, legal clinics, "
            "resume workshops, board meetings, etc.) are written to "
            "data/manual_review/excluded_non_family_events.json instead. "
            "Safe: aborts filtering if >50%% of events would be removed. "
            "Combine with --repair-published-week to clean an existing week."
        ),
    )
    parser.add_argument(
        "--improve-cost-inference",
        action="store_true",
        dest="improve_cost",
        help=(
            "Print a detailed cost inference breakdown after the pipeline run. "
            "With --repair-published-week: also shows before/after cost changes "
            "and writes data/manual_review/cost_inference_report.json."
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

    # Repair mode — short-circuit the normal pipeline
    if args.repair_week is not None:
        # --repair-published-week DATE  →  args.repair_week = "YYYY-MM-DD"
        # --repair-published-week       →  args.repair_week = "" (uses --week-start)
        raw_repair_date = args.repair_week or args.week_start
        if not raw_repair_date:
            logging.error(
                "--repair-published-week requires either a DATE argument "
                "or --week-start DATE"
            )
            return 1
        try:
            repair_date = date.fromisoformat(raw_repair_date)
        except ValueError:
            logging.error(
                "Invalid repair date: %r (expected YYYY-MM-DD)", raw_repair_date
            )
            return 1

        if args.enrich_geo:
            print(f"\n=== NoVA Kids Geo-Repair: {repair_date} ===\n")
            return _repair_geo_enrich(
                repair_date,
                dry_run=args.dry_run,
                strict_region=getattr(args, "strict_region", False),
            )
        else:
            print(f"\n=== NoVA Kids Repair Mode: {repair_date} ===\n")
            return _repair_published_week(
                repair_date,
                dry_run=args.dry_run,
                filter_non_family=getattr(args, "filter_non_family", False),
                improve_cost=getattr(args, "improve_cost", False),
            )

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
    geo_stats = None

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

    # 3.5 Geocode (optional)
    if args.enrich_geo:
        print("\n[geo] Geocoding events...")
        from enrichment.geocode import geocode_events, load_cache
        cache = load_cache()
        print(f"      Cache: {cache.size} existing entries")
        events, geo_stats = geocode_events(events, cache)
        geo_stats.print_summary()

    # 4.5 Family relevance filter (optional)
    n_excluded_non_family = 0
    if getattr(args, "filter_non_family", False):
        print("\n[fam] Family relevance filtering...")
        before_filter = list(events)
        events, excluded_nf = _filter_for_family_feed(events, dry_run=args.dry_run)
        n_excluded_non_family = len(excluded_nf)
        _print_filter_summary(before_filter, excluded_nf)

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
        _sync_public()

    # Summary
    elapsed = (datetime.now(tz=timezone.utc) - started_at).total_seconds()
    print(f"\n=== Done in {elapsed:.1f}s ===")
    if args.use_dullesmoms or args.reprocess:
        print(f"    Discovered:         {n_discovered}")
        print(f"    Resolved:           {n_resolved}")
    print(f"    Published:          {len(events)}")
    if n_excluded_non_family:
        print(f"    Excluded (non-family): {n_excluded_non_family}")
    if args.use_dullesmoms or args.reprocess:
        print(f"    Manual review:      {n_manual_review}")
    print(f"    Duplicates removed: {n_duplicates_removed}")

    # Address & map quality stats
    n_with_address = sum(1 for e in events if e.location_address)
    n_with_city    = sum(1 for e in events if e.city)
    n_with_coords  = sum(1 for e in events if e.latitude is not None)
    n_mappable     = sum(1 for e in events if e.is_mappable)
    n_virtual      = sum(1 for e in events if "virtual" in (e.tags or []))
    print(f"\n  Address & map quality:")
    print(f"    Events with address:    {n_with_address}/{len(events)}")
    print(f"    Events with city:       {n_with_city}/{len(events)}")
    print(f"    Events with coords:     {n_with_coords}/{len(events)}")
    print(f"    Mappable events:        {n_mappable}/{len(events)}")
    print(f"    Virtual (non-map):      {n_virtual}")

    if geo_stats is not None:
        pct = geo_stats.total_mappable / geo_stats.total * 100 if geo_stats.total else 0
        print(f"    Newly geocoded:        {geo_stats.newly_geocoded}")
        print(f"    Cache hits:            {geo_stats.cache_hits}")
        print(f"    Failed geocodes:       {geo_stats.failed}")
        print(f"    Map coverage:          {geo_stats.total_mappable}/{geo_stats.total} ({pct:.0f}%)")

    # Cost breakdown
    _print_cost_summary(events)

    # Source breakdown
    source_counts: dict[str, int] = {}
    for e in events:
        source_counts[e.source_name] = source_counts.get(e.source_name, 0) + 1
    print(f"\n  Events by source:")
    for name, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"    {count:4d}  {name}")
    print()

    if getattr(args, "improve_cost", False):
        _print_cost_inference_detail(events)

    return 0


if __name__ == "__main__":
    sys.exit(main())
