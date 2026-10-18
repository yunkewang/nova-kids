"""
Scraper for NOVA Parks (Northern Virginia Regional Park Authority) events.

Source: https://www.novaparks.com/events
Type:   HTML (BeautifulSoup) — custom AJAX endpoint, server-side rendered

The events listing is NOT standard Drupal Views AJAX.  Event rows are served
by a custom endpoint:

    GET https://www.novaparks.com/ajax-event-calendar
        ?from={YYYY-MM-DD} 00:00:01
        &to={YYYY-MM-DD} 23:59:59
        &category={All|32|36|...}
        &response_type=ajax

which returns full server-rendered ``div.views-row`` event teasers.  There is
no pager — the caller iterates date windows (30-day chunks) across the
pipeline horizon instead.

Category is fetched as ``All``; family/children relevance is decided by the
pipeline's own family_relevance classifier (enrichment/family_relevance.py),
which already scores park-authority sources.  The scraper only excludes
explicitly adult-only listings (21+, alcohol, etc.).

Row markup (verified live 2026-10-18):
    div.views-row
      .date-detail  > span.date > span.month / span.day [- span.month / span.day]
                    e.g. "Oct 16 - 18" or "Oct 30 - Nov 1" (no year on the page)
      .copy > h3 > a[href]      title + detail URL
      strong.time               e.g. "6:00pm - 10:00am"
      a.facility-link           e.g. "Pohick Bay Regional Park, Lorton, VA"
      .copy > div > p           summary text
      img[data-src]             real image; src is a 1x1 gif placeholder

The year is resolved from the request window the row was fetched in: a row
showing "Oct 16" inside the Oct-2026 window belongs to 2026; a "Jan 5" row
inside a Dec-2026 window rolls over to 2027 (see _resolve_date).
"""

from __future__ import annotations

import calendar
import logging
import re
from datetime import date, timedelta
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from scrapers.base import BaseScraper
from scrapers.detail_image import extract_image_from_detail_html
from scrapers.detail_price import (
    extract_price_from_detail_html,
    fetch_detail_html_text,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.novaparks.com"
CALENDAR_URL = f"{BASE_URL}/ajax-event-calendar"

# Date-window chunking: the endpoint has no pager, so the pipeline horizon is
# covered in non-overlapping chunks.  Matches the curated source's 120-day
# horizon (see config/curated_events.yaml handling).
HORIZON_DAYS = 120
WINDOW_DAYS = 30

# Generous timeout — the first request of a session can be slow (~30s).
AJAX_TIMEOUT = 90

# Cap detail-page lookups; the calendar typically surfaces a few hundred rows
# once recurring events are expanded (one row per occurrence).
DETAIL_FETCH_LIMIT = 500

# Explicit adult-only exclusions — skip if any of these match.  Everything
# else passes through to the pipeline's family_relevance classifier.
_ADULT_ONLY_RE = re.compile(
    r"\b(?:adult only|21\+|beer|brews?|wine\s+tasting|spirits|cocktail|happy\s+hour|"
    r"corporate\s+event|wedding|networking|golf\s+tournament|gun|firearm|"
    r"shooting\s+range|fun\s+shoot)\b",
    re.IGNORECASE,
)

# Date text forms: "Oct 16", "Oct 16 - 18", "Oct 30 - Nov 1"
_DATE_RANGE_RE = re.compile(
    r"([A-Za-z]{3,9})\s+(\d{1,2})"                 # start month + day
    r"(?:\s*-\s*(?:([A-Za-z]{3,9})\s+)?(\d{1,2}))?"  # optional "- [end month] end day"
)

_MONTH_NUM = {
    name.lower(): num
    for num, name in enumerate(calendar.month_abbr)
    if name
}
_MONTH_NUM.update({
    name.lower(): num
    for num, name in enumerate(calendar.month_name)
    if name
})


def _month_number(name: str) -> int | None:
    return _MONTH_NUM.get(name.strip().lower()[:3])


def _resolve_date(
    month: int, day: int, window_from: date
) -> date | None:
    """
    Resolve a month/day (no year, as shown on the list rows) to a real date.

    The year is inferred from the request window the row was fetched in.  The
    endpoint also returns events that merely *overlap* the window (e.g. an
    "Oct 16 - 18" row inside a window starting Oct 18), so a date is only
    rolled into the following year when it falls more than one window-length
    before the window start.  This keeps overlapping multi-day events in the
    correct year while still handling the Dec->Jan rollover.  Leap-day edge
    cases fall forward to the next valid year.
    """
    earliest = window_from - timedelta(days=WINDOW_DAYS)
    for year in (window_from.year, window_from.year + 1, window_from.year + 2):
        try:
            candidate = date(year, month, day)
        except ValueError:
            continue  # e.g. Feb 29 in a non-leap year — try the next year
        if candidate >= earliest:
            return candidate
    return None


def _parse_date_text(
    date_text: str, window_from: date
) -> tuple[date | None, date | None]:
    """
    Parse a row's date text into (start_date, end_date).

    Handles "Oct 16" (end == start), "Oct 16 - 18", and month-crossing ranges
    like "Oct 30 - Nov 1".  Returns (None, None) when nothing parseable is
    found.
    """
    m = _DATE_RANGE_RE.search(date_text)
    if not m:
        return None, None

    start_month = _month_number(m.group(1))
    start_day = int(m.group(2))
    if start_month is None:
        return None, None

    start = _resolve_date(start_month, start_day, window_from)
    if start is None:
        return None, None

    end_month_name = m.group(3)
    end_day = int(m.group(4)) if m.group(4) else None
    if not end_day:
        return start, start  # single-day event

    end_month = _month_number(end_month_name) if end_month_name else start_month
    if end_month is None:
        return start, start

    # End date is on/after the start date; roll the year forward if the
    # window-based resolution landed it earlier (e.g. "Dec 30 - Jan 1").
    end = _resolve_date(end_month, end_day, window_from)
    if end is None:
        return start, start
    while end < start:
        try:
            end = date(end.year + 1, end.month, end.day)
        except ValueError:
            break
    return start, end


def _split_time(time_text: str | None) -> tuple[str | None, str | None]:
    """Split "6:00pm - 10:00am" into (start, end); tolerates a single time."""
    if not time_text:
        return None, None
    parts = [p.strip() for p in time_text.split("-", 1)]
    if len(parts) == 2 and parts[1]:
        return parts[0] or None, parts[1]
    return parts[0] or None, None


def _image_url_from_row(row: Tag) -> str | None:
    """
    Extract the row's image URL.

    The <img> src is always a 1x1 gif placeholder — the real URL lives in
    data-src (with a srcset for other sizes, which we don't need).
    """
    img = row.select_one("img")
    if img is None:
        return None
    data_src = img.get("data-src")
    if not data_src:
        return None
    return data_src if data_src.startswith("http") else urljoin(BASE_URL, data_src)


def _is_adult_only(title: str, summary: str | None) -> bool:
    return bool(_ADULT_ONLY_RE.search(f"{title} {summary or ''}"))


class NoVAParksScraper(BaseScraper):
    """
    Scrapes events from the NOVA Parks AJAX event calendar.

    Fetches category=All in 30-day windows across the 120-day pipeline
    horizon, parses the server-rendered rows, and enriches each record with
    detail-page pricing (listing cards carry no dollar amounts).
    """

    source_id = "nova_parks"
    source_name = "NOVA Parks"

    def __init__(self) -> None:
        super().__init__()
        self._detail_fetches = 0

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------

    def _window_url(self, start: date, end: date) -> str:
        return (
            f"{CALENDAR_URL}"
            f"?from={start:%Y-%m-%d}%2000:00:01"
            f"&to={end:%Y-%m-%d}%2023:59:59"
            f"&category=All&response_type=ajax"
        )

    def _fetch_window(self, start: date, end: date) -> list[Tag]:
        """Fetch one date window; return the event row tags (may be empty)."""
        url = self._window_url(start, end)
        logger.debug("NOVA Parks: fetching window %s -> %s", start, end)
        try:
            resp = self.get(url, timeout=AJAX_TIMEOUT)
        except Exception as exc:
            logger.warning(
                "NOVA Parks: window %s -> %s failed: %s", start, end, exc
            )
            return []
        soup = BeautifulSoup(resp.text, "lxml")
        return soup.select("div.views-row")

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_row(
        self, row: Tag, window_from: date
    ) -> dict[str, Any] | None:
        title_el = row.select_one(".copy h3 a") or row.select_one("h3 a")
        if title_el is None:
            return None
        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        if not title or not href:
            return None
        event_url = href if href.startswith("http") else urljoin(BASE_URL, href)
        if "/events/" not in event_url:
            return None

        # Date — year resolved from the request window
        date_el = row.select_one(".date-detail")
        date_text = date_el.get_text(separator=" ", strip=True) if date_el else None
        start_date, end_date = (
            _parse_date_text(date_text, window_from) if date_text else (None, None)
        )
        if start_date is None:
            logger.debug("NOVA Parks: skipping %r — unparseable date %r", title, date_text)
            return None

        # Time
        time_el = row.select_one("strong.time")
        t_start, t_end = _split_time(
            time_el.get_text(strip=True) if time_el else None
        )

        # Location, e.g. "Pohick Bay Regional Park, Lorton, VA"
        loc_el = row.select_one("a.facility-link")
        location_text = loc_el.get_text(strip=True) if loc_el else None

        # Summary
        summary_el = row.select_one(".copy > div > p")
        summary_text = (
            summary_el.get_text(separator=" ", strip=True) or None
            if summary_el
            else None
        )
        if summary_text and len(summary_text) > 600:
            summary_text = summary_text[:600]

        if _is_adult_only(title, summary_text):
            logger.debug("NOVA Parks: skipping adult-only %r", title)
            return None

        image_url = _image_url_from_row(row)

        # Resolved start/end for normalize_record(); date_text stays
        # source-faithful for debugging.
        start_text = f"{start_date:%Y-%m-%d}" + (f", {t_start}" if t_start else "")
        end_text = f"{end_date:%Y-%m-%d}" + (f", {t_end}" if t_end else "")

        return {
            "source_id":     self.source_id,
            "source_name":   self.source_name,
            "source_url":    event_url,
            "title":         title,
            "date_text":     date_text,
            "start_text":    start_text,
            "end_text":      end_text,
            "location_text": location_text,
            "summary_text":  summary_text,
            "image_url":     image_url,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_raw(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        self._detail_fetches = 0

        today = date.today()
        for offset in range(0, HORIZON_DAYS, WINDOW_DAYS):
            window_from = today + timedelta(days=offset)
            window_to = min(
                window_from + timedelta(days=WINDOW_DAYS - 1),
                today + timedelta(days=HORIZON_DAYS - 1),
            )
            for row in self._fetch_window(window_from, window_to):
                raw = self._parse_row(row, window_from)
                if not raw:
                    continue
                # Recurring events are served as one row per occurrence day
                # with the same URL, so dedupe on (url, start_date) — each
                # occurrence is a real, separately-attendable listing.  This
                # also drops rows that merely overlap adjacent windows.
                key = (raw["source_url"], raw["start_text"])
                if key in seen:
                    continue
                seen.add(key)
                records.append(raw)

        # Detail-page pricing + image fallback.  The list cards carry no
        # dollar amounts, so without this step paid programs collapse into
        # the "default free" fallback.  The detail-page HTML is fetched once
        # and shared between the pricing and image extractors.
        for r in records:
            url = r.get("source_url")
            if not url:
                continue
            detail_html: str | None = None
            if not r.get("price_text"):
                detail_html = fetch_detail_html_text(
                    self, url,
                    fetch_count_attr="_detail_fetches",
                    limit=DETAIL_FETCH_LIMIT,
                )
                r["price_text"] = extract_price_from_detail_html(detail_html)
            if not r.get("image_url"):
                if detail_html is None:
                    detail_html = fetch_detail_html_text(
                        self, url,
                        fetch_count_attr="_detail_fetches",
                        limit=DETAIL_FETCH_LIMIT,
                    )
                r["image_url"] = extract_image_from_detail_html(detail_html, url)

        logger.info(
            "NOVA Parks: parsed %d events, fetched %d detail pages",
            len(records), self._detail_fetches,
        )
        return records
