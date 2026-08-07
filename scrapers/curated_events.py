"""
Curated marquee-events source for the NoVA Kids pipeline.

Why this source exists
----------------------
The scraped sources (libraries, parks & rec) are very good at recurring
*programming* — storytimes, nature walks, rec-center classes. They are
structurally incapable of surfacing the other half of family life in Northern
Virginia: the annual and seasonal *outings* a family plans a weekend around.
County fairs, the state fair, a circus that pitches a tent for three weeks,
a farm's eight-week fall festival, holiday light trails.

Those events almost never appear on a scrapeable municipal calendar. They live
on a single-purpose promotional website — often JavaScript-rendered, often with
no per-occurrence markup at all — that changes once a year. Writing a scraper
per circus is not maintainable; a human-verified entry that a maintainer
refreshes annually is.

Contract
--------
This source reads ``config/curated_events.yaml``. It performs **no HTTP** —
every field is transcribed by a human from the event's official page, and
``source_url`` always points at that original page (never an aggregator, never
a ticket reseller). See ``docs/source_rules.md`` for the governing policy.

Each YAML entry describes an event *run*, which this module expands into one
raw record per calendar day the event is open. Per-day expansion (rather than
per-showtime) matches the pipeline's dedupe fingerprint, which keys on
title + start **date** + location — multiple showtimes on one day would
collapse into a single record anyway, so the day's full operating window is
carried in ``start``/``end`` and specific showtimes belong in the summary.

Occurrences in the past, and occurrences beyond ``HORIZON_DAYS``, are dropped
at scrape time so a stale YAML file degrades to "no events" rather than to
"wrong events".
"""

from __future__ import annotations

import datetime as _dt
import logging
from typing import Any

import yaml

from config.settings import CONFIG_DIR
from scrapers.base import BaseScraper, ScraperError

logger = logging.getLogger(__name__)

CURATED_FILE = CONFIG_DIR / "curated_events.yaml"

#: Occurrences further out than this are not published. Keeps the feed to a
#: planning horizon families actually use, and limits the blast radius of a
#: date that was transcribed wrong.
HORIZON_DAYS = 120

#: A curated entry whose ``verified_on`` is older than this is logged as a
#: warning — the underlying page may have changed since a human last read it.
STALE_AFTER_DAYS = 180

_WEEKDAYS: dict[str, int] = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6,
}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _as_date(value: Any, *, field: str, slug: str) -> _dt.date:
    """Coerce a YAML scalar to a date. PyYAML already gives us date objects."""
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        try:
            return _dt.date.fromisoformat(value.strip())
        except ValueError as exc:
            raise ScraperError(
                f"curated_events: entry '{slug}' has an unparseable {field}: {value!r}"
            ) from exc
    raise ScraperError(
        f"curated_events: entry '{slug}' is missing a valid {field} (got {value!r})"
    )


def _as_time(value: Any, *, field: str, slug: str) -> _dt.time | None:
    """Coerce ``"19:00"`` / ``"9:30"`` to a time. None means 'no time given'."""
    if value is None or value == "":
        return None
    if isinstance(value, _dt.time):
        return value
    if isinstance(value, str):
        text = value.strip()
        for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I%p", "%I:%M%p"):
            try:
                return _dt.datetime.strptime(text.upper(), fmt.upper()).time()
            except ValueError:
                continue
    raise ScraperError(
        f"curated_events: entry '{slug}' has an unparseable {field}: {value!r}"
    )


def _parse_days_of_week(value: Any, slug: str) -> set[int] | None:
    """``[fri, sat, sun]`` → ``{4, 5, 6}``. None means 'every day'."""
    if not value:
        return None
    days: set[int] = set()
    for item in value:
        key = str(item).strip().lower()[:3]
        if key not in _WEEKDAYS:
            raise ScraperError(
                f"curated_events: entry '{slug}' has an unknown day_of_week: {item!r}"
            )
        days.add(_WEEKDAYS[key])
    return days


def _occurrence_dates(entry: dict[str, Any], slug: str) -> list[dict[str, Any]]:
    """
    Expand an entry's schedule into ``[{date, start_time, end_time, note}, …]``.

    Supports two mutually exclusive forms:

    ``dates:``  explicit per-day list — use when each day has its own hours
                (county fairs typically do).
    ``run:``    a start/end date range with one set of hours, optionally
                restricted to certain weekdays.
    """
    explicit = entry.get("dates")
    run = entry.get("run")

    if explicit and run:
        raise ScraperError(
            f"curated_events: entry '{slug}' sets both 'dates' and 'run' — pick one."
        )

    occurrences: list[dict[str, Any]] = []

    if explicit:
        for item in explicit:
            occurrences.append({
                "date": _as_date(item.get("date"), field="dates[].date", slug=slug),
                "start_time": _as_time(item.get("start_time"), field="start_time", slug=slug),
                "end_time": _as_time(item.get("end_time"), field="end_time", slug=slug),
                "note": item.get("note"),
            })
        return occurrences

    if not run:
        raise ScraperError(
            f"curated_events: entry '{slug}' has neither 'dates' nor 'run'."
        )

    start_date = _as_date(run.get("start_date"), field="run.start_date", slug=slug)
    end_date = _as_date(run.get("end_date", run.get("start_date")),
                        field="run.end_date", slug=slug)
    if end_date < start_date:
        raise ScraperError(
            f"curated_events: entry '{slug}' has run.end_date before run.start_date."
        )

    start_time = _as_time(run.get("start_time"), field="run.start_time", slug=slug)
    end_time = _as_time(run.get("end_time"), field="run.end_time", slug=slug)
    days = _parse_days_of_week(run.get("days_of_week"), slug)

    current = start_date
    while current <= end_date:
        if days is None or current.weekday() in days:
            occurrences.append({
                "date": current,
                "start_time": start_time,
                "end_time": end_time,
                "note": None,
            })
        current += _dt.timedelta(days=1)

    return occurrences


def _location_text(entry: dict[str, Any]) -> str | None:
    """Build the ``"Venue, Street, City, ST ZIP"`` string normalize() expects."""
    parts = [entry.get("venue"), entry.get("address")]
    joined = ", ".join(p.strip() for p in parts if p and str(p).strip())
    return joined or None


def _warn_if_stale(entry: dict[str, Any], slug: str, today: _dt.date) -> None:
    verified = entry.get("verified_on")
    if verified is None:
        logger.warning(
            "curated_events: entry '%s' has no verified_on date — "
            "cannot tell how fresh its details are.", slug,
        )
        return
    verified_date = _as_date(verified, field="verified_on", slug=slug)
    age = (today - verified_date).days
    if age > STALE_AFTER_DAYS:
        logger.warning(
            "curated_events: entry '%s' was last verified %d days ago (%s) — "
            "re-check the official page before trusting its dates and pricing.",
            slug, age, verified_date.isoformat(),
        )


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class CuratedEventsScraper(BaseScraper):
    """Expands human-verified marquee events from config/curated_events.yaml."""

    source_id = "curated_marquee_events"
    source_name = "NoVA Kids Curated Events"

    def fetch_raw(self) -> list[dict[str, Any]]:
        if not CURATED_FILE.exists():
            logger.warning("curated_events: %s not found — no events.", CURATED_FILE)
            return []

        try:
            document = yaml.safe_load(CURATED_FILE.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ScraperError(f"curated_events: could not parse {CURATED_FILE}: {exc}") from exc

        entries = document.get("events") or []
        today = _dt.date.today()
        horizon = today + _dt.timedelta(days=HORIZON_DAYS)

        records: list[dict[str, Any]] = []
        skipped_past = 0
        skipped_future = 0

        for entry in entries:
            slug = str(entry.get("slug") or entry.get("title") or "<unnamed>")

            if entry.get("enabled") is False:
                logger.debug("curated_events: entry '%s' is disabled — skipping.", slug)
                continue

            title = entry.get("title")
            source_url = entry.get("source_url")
            if not title or not source_url:
                raise ScraperError(
                    f"curated_events: entry '{slug}' must set both 'title' and 'source_url'."
                )

            _warn_if_stale(entry, slug, today)

            location_text = _location_text(entry)
            summary = entry.get("summary")
            base_tags = [str(t) for t in (entry.get("tags") or [])]

            for occ in _occurrence_dates(entry, slug):
                occ_date: _dt.date = occ["date"]
                if occ_date < today:
                    skipped_past += 1
                    continue
                if occ_date > horizon:
                    skipped_future += 1
                    continue

                start_time: _dt.time | None = occ["start_time"]
                end_time: _dt.time | None = occ["end_time"]
                all_day = start_time is None

                start_text = (
                    _dt.datetime.combine(occ_date, start_time).isoformat()
                    if start_time is not None
                    else occ_date.isoformat()
                )
                end_text = (
                    _dt.datetime.combine(occ_date, end_time).isoformat()
                    if end_time is not None and start_time is not None
                    else None
                )

                # A per-day note ("Sensory-friendly hours 10am–1pm") is appended
                # to the verified summary rather than replacing it.
                day_summary = summary
                if occ.get("note"):
                    day_summary = f"{summary} {occ['note']}".strip() if summary else occ["note"]

                records.append({
                    "source_id":        self.source_id,
                    "source_name":      entry.get("organizer") or self.source_name,
                    "source_url":       source_url,
                    "title":            title,
                    "start_text":       start_text,
                    "end_text":         end_text,
                    "all_day":          all_day,
                    "location_text":    location_text,
                    "summary_text":     day_summary,
                    "price_text":       entry.get("price_text"),
                    "registration_url": entry.get("registration_url"),
                    "image_url":        entry.get("image_url"),
                    "city":             entry.get("city"),
                    "county":           entry.get("county"),
                    "age_min":          entry.get("age_min"),
                    "age_max":          entry.get("age_max"),
                    "extra_tags":       base_tags,
                })

        logger.info(
            "curated_events: %d entries → %d occurrences "
            "(%d past, %d beyond %d-day horizon)",
            len(entries), len(records), skipped_past, skipped_future, HORIZON_DAYS,
        )
        return records
