"""
Deduplication logic for the NoVA Kids pipeline.

Strategy
--------
Two events are considered duplicates when they share the same stable hash key:
    sha256( title_normalized | start_date | location_name_normalized | source_url )[:16]

This is the same key used to generate Event.id in normalize.py.

When duplicates are found we keep the record with the highest quality score,
determined by `_quality_score()`.

Public entry point: deduplicate(events: list[Event]) -> list[Event]
"""

from __future__ import annotations

import logging
from typing import NamedTuple

from config.schema import Event

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Quality scoring
# ---------------------------------------------------------------------------

class _QualityFactors(NamedTuple):
    has_summary: bool
    has_image: bool
    has_registration_url: bool
    has_end: bool
    has_location: bool
    tag_count: int
    family_score: float


def _quality_score(event: Event) -> float:
    """
    Assign a numeric quality score to an event for duplicate resolution.

    Higher is better.  Weighted sum of boolean completeness flags and
    the family_friendly_score.
    """
    factors = _QualityFactors(
        has_summary=bool(event.summary),
        has_image=bool(event.image_url),
        has_registration_url=bool(event.registration_url),
        has_end=event.end is not None,
        has_location=bool(event.location_name),
        tag_count=len(event.tags),
        family_score=event.family_friendly_score,
    )

    score = (
        (0.25 if factors.has_summary else 0.0)
        + (0.15 if factors.has_image else 0.0)
        + (0.10 if factors.has_registration_url else 0.0)
        + (0.10 if factors.has_end else 0.0)
        + (0.10 if factors.has_location else 0.0)
        + (0.05 * min(factors.tag_count, 4))  # up to 0.20 for >=4 tags
        + (0.10 * factors.family_score)
    )
    return round(score, 6)


# ---------------------------------------------------------------------------
# Alternate-key fingerprint (cross-source duplicates)
# ---------------------------------------------------------------------------

def _fingerprint(event: Event) -> str:
    """
    Compute a loose fingerprint for cross-source duplicate detection.

    Two events that share title + start date (date only, not time) +
    location_name are treated as potential duplicates even when fetched
    from different sources.

    This is intentionally coarser than Event.id to catch cases like:
        • County parks site and Eventbrite both listing the same festival.
    """
    title_key = event.title.lower().strip()
    date_key = event.start.date().isoformat()
    loc_key = (event.location_name or "").lower().strip()
    return f"{title_key}|{date_key}|{loc_key}"


# ---------------------------------------------------------------------------
# Public deduplication function
# ---------------------------------------------------------------------------

def deduplicate(events: list[Event]) -> list[Event]:
    """
    Remove duplicate events, keeping the highest-quality record per duplicate group.

    Two passes:
      1. Exact-ID duplicates (same source, same hash).
      2. Cross-source fingerprint duplicates (same title + date + location).

    Returns a sorted list (by start datetime, then title).
    """
    # ---- Pass 1: exact ID dedup ----------------------------------------
    by_id: dict[str, Event] = {}
    for event in events:
        existing = by_id.get(event.id)
        if existing is None:
            by_id[event.id] = event
        else:
            if _quality_score(event) > _quality_score(existing):
                logger.debug(
                    "Replacing lower-quality duplicate id=%s ('%s')",
                    event.id, event.title,
                )
                by_id[event.id] = event
            else:
                logger.debug(
                    "Dropping duplicate id=%s ('%s') in favour of existing record.",
                    event.id, event.title,
                )

    # ---- Pass 2: cross-source fingerprint dedup -------------------------
    by_fingerprint: dict[str, Event] = {}
    for event in by_id.values():
        fp = _fingerprint(event)
        existing = by_fingerprint.get(fp)
        if existing is None:
            by_fingerprint[fp] = event
        else:
            if _quality_score(event) > _quality_score(existing):
                logger.debug(
                    "Cross-source dedup: replacing '%s' (source=%s) with '%s' (source=%s)",
                    existing.title, existing.source_name,
                    event.title, event.source_name,
                )
                by_fingerprint[fp] = event
            else:
                logger.debug(
                    "Cross-source dedup: dropping '%s' (source=%s) in favour of '%s' (source=%s)",
                    event.title, event.source_name,
                    existing.title, existing.source_name,
                )

    deduplicated = list(by_fingerprint.values())

    logger.info(
        "Deduplication: %d input → %d output (%d removed)",
        len(events),
        len(deduplicated),
        len(events) - len(deduplicated),
    )

    def _sort_key(e: "Event") -> tuple:
        # Normalize to naive UTC for sorting — avoids TypeError when mixing
        # offset-aware and offset-naive datetimes across scrapers / seed resolvers.
        from datetime import timezone as _tz
        start = e.start
        if start.tzinfo is not None:
            start = start.astimezone(_tz.utc).replace(tzinfo=None)
        return (start, e.title)

    return sorted(deduplicated, key=_sort_key)
