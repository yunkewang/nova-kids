"""
Weekly publishing logic for the NoVA Kids pipeline.

Produces:
  data/published/events/week-YYYY-MM-DD.json   (one per ISO week)
  data/published/events/index.json             (manifest of all weeks)

The week date in the filename is the Monday (ISO week start) of the week
that contains the earliest event in the batch — or the current week if
no events exist.

Public entry point: publish_events(events: list[Event]) -> PublishResult
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config.schema import Event
from config.settings import PUBLISHED_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes for the output format
# ---------------------------------------------------------------------------

@dataclass
class WeeklyFile:
    week_start: str       # ISO date, e.g. "2025-06-02"
    generated_at: str     # ISO datetime UTC
    source_count: int
    event_count: int
    events: list[dict[str, Any]]


@dataclass
class IndexFile:
    version: str
    generated_at: str
    available_weeks: list[str]   # sorted ISO date strings
    latest_week: str


@dataclass
class PublishResult:
    week_start: date
    output_path: Path
    index_path: Path
    event_count: int
    source_count: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VERSION = "1"


def _week_monday(dt: date) -> date:
    """Return the Monday of the ISO week containing `dt`."""
    return dt - timedelta(days=dt.weekday())


def _week_filename(monday: date) -> str:
    return f"week-{monday.isoformat()}.json"


def _event_to_dict(event: Event) -> dict[str, Any]:
    """Serialize an Event to a JSON-safe dict."""
    data = event.model_dump()
    # Convert datetime objects to ISO strings
    for key in ("start", "end", "last_verified_at"):
        if data.get(key) is not None:
            data[key] = data[key].isoformat()
    return data


def _load_existing_index() -> IndexFile | None:
    """Load the existing index.json if present."""
    index_path = PUBLISHED_DIR / "index.json"
    if not index_path.exists():
        return None
    try:
        raw = json.loads(index_path.read_text(encoding="utf-8"))
        return IndexFile(
            version=raw.get("version", VERSION),
            generated_at=raw.get("generated_at", ""),
            available_weeks=raw.get("available_weeks", []),
            latest_week=raw.get("latest_week", ""),
        )
    except Exception as exc:
        logger.warning("Could not load existing index.json: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def publish_events(
    events: list[Event],
    week_start: date | None = None,
) -> PublishResult:
    """
    Write (or overwrite) the weekly JSON file and update index.json.

    If `week_start` is provided it is used as-is (snapped to Monday).
    Otherwise the week is inferred from the earliest event start time,
    or falls back to the current week when there are no events.
    """
    now_utc = datetime.now(tz=timezone.utc)

    # Determine which week we're publishing
    if week_start is not None:
        week_start = _week_monday(week_start)
    elif events:
        earliest_start = min(e.start for e in events)
        week_start = _week_monday(
            earliest_start.date() if isinstance(earliest_start, datetime) else earliest_start
        )
    else:
        week_start = _week_monday(now_utc.date())

    # Collect unique source names
    source_names = sorted({e.source_name for e in events})

    # Build the weekly payload
    weekly = WeeklyFile(
        week_start=week_start.isoformat(),
        generated_at=now_utc.isoformat(),
        source_count=len(source_names),
        event_count=len(events),
        events=[_event_to_dict(e) for e in events],
    )

    # Write the weekly file
    out_filename = _week_filename(week_start)
    out_path = PUBLISHED_DIR / out_filename
    out_path.write_text(
        json.dumps(asdict(weekly), indent=2, default=str),
        encoding="utf-8",
    )
    logger.info("Published %d events to %s", len(events), out_path)

    # Update index.json
    existing_index = _load_existing_index()
    available_weeks: list[str] = list(
        existing_index.available_weeks if existing_index else []
    )
    week_key = week_start.isoformat()
    if week_key not in available_weeks:
        available_weeks.append(week_key)
    available_weeks = sorted(set(available_weeks))

    # Also scan the directory for any week files not yet in the index
    for p in PUBLISHED_DIR.glob("week-*.json"):
        stem = p.stem.replace("week-", "")
        if stem not in available_weeks:
            available_weeks.append(stem)
    available_weeks = sorted(set(available_weeks))

    latest_week = available_weeks[-1] if available_weeks else week_key

    index = IndexFile(
        version=VERSION,
        generated_at=now_utc.isoformat(),
        available_weeks=available_weeks,
        latest_week=latest_week,
    )
    index_path = PUBLISHED_DIR / "index.json"
    index_path.write_text(
        json.dumps(asdict(index), indent=2),
        encoding="utf-8",
    )
    logger.info("Updated index at %s", index_path)

    return PublishResult(
        week_start=week_start,
        output_path=out_path,
        index_path=index_path,
        event_count=len(events),
        source_count=len(source_names),
    )
