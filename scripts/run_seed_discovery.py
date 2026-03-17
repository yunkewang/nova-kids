#!/usr/bin/env python3
"""
NoVA Kids — Seed Discovery CLI

Runs the DullesMoms seed finder to discover candidate events, then attempts
to resolve each candidate to its original event host page.

Resolved events are normalized and saved to data/normalized/seed_events.json
for inspection before the next pipeline publish run.

Unresolved candidates are saved to data/manual_review/pending_candidates.json
for human review.

Usage:
    python scripts/run_seed_discovery.py [OPTIONS]

Options:
    --dry-run       Discover and resolve but do not write any files.
    --no-resolve    Only run seed discovery (no HTTP fetches to original pages).
    --verbose, -v   Enable DEBUG logging.

Typical workflow:
    1. Run this script to populate the manual review queue and seed_events.json.
    2. Review data/manual_review/pending_candidates.json.
    3. Approve candidates manually (set status to 'manual_review_approved').
    4. Run scripts/run_pipeline.py to normalize, dedupe, validate, and publish.

LEGAL NOTE:
    This script visits DullesMoms only as a discovery layer.
    It does NOT copy or publish DullesMoms descriptions, summaries, or images.
    The source_name and source_url in all published events will be set to the
    original event host, not DullesMoms.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import MANUAL_REVIEW_DIR, NORMALIZED_DIR
from models.candidate import CandidateEvent, CandidateStatus
from seed_discovery.dullesmoms_seed_finder import DullesMomsSeedFinder
from seed_discovery.resolver import resolve_candidates


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Manual review persistence
# ---------------------------------------------------------------------------

def _load_existing_review_queue() -> list[dict]:
    """Load any existing pending candidates so we can merge/deduplicate."""
    path = MANUAL_REVIEW_DIR / "pending_candidates.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logging.warning("Could not load existing review queue: %s", exc)
        return []


def save_manual_review_queue(
    new_candidates: list[CandidateEvent],
    dry_run: bool = False,
) -> Path:
    """
    Merge new candidates into the existing review queue and save.

    Deduplicates by candidate_id so re-running won't create duplicates.
    Preserves any existing records that have been manually annotated.
    """
    existing = {r["candidate_id"]: r for r in _load_existing_review_queue()}

    for candidate in new_candidates:
        cid = candidate.candidate_id
        if cid not in existing:
            # New candidate — add to queue
            existing[cid] = candidate.model_dump()
        else:
            # Already in queue — update only automatic fields, preserve manual notes
            existing[cid]["confidence"] = candidate.confidence
            existing[cid]["status"] = candidate.status.value
            # Preserve any human-added notes
            if existing[cid].get("status") not in ("manual_review_approved", "rejected"):
                existing[cid]["notes"] = candidate.notes

    queue = list(existing.values())
    path = MANUAL_REVIEW_DIR / "pending_candidates.json"

    if not dry_run:
        path.write_text(
            json.dumps(queue, indent=2, default=str),
            encoding="utf-8",
        )
        logging.info("Manual review queue: %d candidates → %s", len(queue), path)
    else:
        logging.info("[dry-run] Would write %d candidates to %s", len(queue), path)

    return path


def save_seed_events(raw_records: list[dict], dry_run: bool = False) -> Path:
    """Save resolved raw records for inspection before the publish pipeline runs."""
    path = NORMALIZED_DIR / "seed_events.json"
    if not dry_run:
        path.write_text(
            json.dumps(raw_records, indent=2, default=str),
            encoding="utf-8",
        )
        logging.info("Seed events: %d records → %s", len(raw_records), path)
    else:
        logging.info("[dry-run] Would write %d seed records to %s", len(raw_records), path)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Discover candidate events from the DullesMoms calendar, "
            "resolve them to original host pages, and route unresolved "
            "items to manual review."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and resolve but do not write any output files.",
    )
    parser.add_argument(
        "--no-resolve",
        action="store_true",
        help="Only run seed discovery; skip fetching original pages.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)

    print("\n=== NoVA Kids — Seed Discovery ===")
    print("NOTE: DullesMoms is used for discovery only.")
    print("      Published content comes from original event host pages.\n")

    started_at = datetime.now(tz=timezone.utc)

    # ---- Step 1: Seed discovery ----------------------------------------
    print("[1/3] Running DullesMoms seed finder...")
    finder = DullesMomsSeedFinder()
    try:
        candidates = finder.run()
    except Exception as exc:
        logging.error("Seed finder failed: %s", exc)
        return 1

    needs_review = [c for c in candidates if c.requires_manual_review]
    auto_resolvable = [c for c in candidates if not c.requires_manual_review]
    print(f"      Candidates discovered: {len(candidates)}")
    print(f"      Auto-resolvable:       {len(auto_resolvable)}")
    print(f"      Needs manual review:   {len(needs_review)}")

    # ---- Step 2: Resolve to original pages -----------------------------
    resolved_raws: list[dict] = []
    newly_flagged: list[CandidateEvent] = list(needs_review)  # copy

    if args.no_resolve:
        print("\n[2/3] Skipping resolution (--no-resolve).")
        newly_flagged = list(candidates)
    else:
        print(f"\n[2/3] Resolving {len(auto_resolvable)} candidates to original pages...")
        resolved_raws, resolution_failures = resolve_candidates(auto_resolvable)
        newly_flagged.extend(resolution_failures)
        print(f"      Resolved successfully: {len(resolved_raws)}")
        print(f"      Sent to manual review: {len(resolution_failures)}")

    # ---- Step 3: Write outputs -----------------------------------------
    print("\n[3/3] Saving outputs...")

    review_path = save_manual_review_queue(newly_flagged, dry_run=args.dry_run)
    seed_path = save_seed_events(resolved_raws, dry_run=args.dry_run)

    elapsed = (datetime.now(tz=timezone.utc) - started_at).total_seconds()
    print(f"\n=== Done in {elapsed:.1f}s ===")
    print(f"    Resolved seed records: {len(resolved_raws)}")
    print(f"      → {seed_path.name}  (run pipeline to publish)")
    print(f"    Manual review queue:   {len(newly_flagged)} candidates")
    print(f"      → {review_path.name}")
    print()
    print("Next steps:")
    print("  1. Review data/manual_review/pending_candidates.json")
    print("  2. Run: python scripts/run_pipeline.py --with-seed-discovery")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
