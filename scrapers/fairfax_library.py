"""
Scraper for Fairfax County Public Library events.

Source: https://librarycalendar.fairfaxcounty.gov/calendar
Type:   JSON API (LibCal/SpringShare AJAX)

The old HTML page (/library/programs-events) no longer exists.  Events are
served via the LibCal AJAX API — this scraper calls it directly.

API endpoint: https://librarycalendar.fairfaxcounty.gov/ajax/calendar/list
Parameters:
    c          = 6524                  (calendar ID)
    date       = YYYY-MM-DD            (start date, today)
    days       = 14                    (look-ahead window)
    audience[] = 2185                  School Age
    audience[] = 2186                  Preschoolers
    audience[] = 2187                  Babies / Toddlers
    offset     = N                     (pagination)

Response JSON: {total_results, perpage, status, results: [...]}
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

LIBCAL_URL = "https://librarycalendar.fairfaxcounty.gov/ajax/calendar/list"
CALENDAR_ID = 6524
AUDIENCE_IDS = [2185, 2186, 2187]  # School Age, Preschoolers, Babies/Toddlers
DAYS_WINDOW = 14
MAX_PAGES = 20  # safety cap


class FairfaxLibraryScraper(BaseScraper):
    """Scrapes children/family events from Fairfax County Public Library via LibCal API."""

    source_id = "fairfax_county_library"
    source_name = "Fairfax County Public Library"

    def fetch_raw(self) -> list[dict[str, Any]]:
        today = date.today().isoformat()
        base_params: list[tuple[str, Any]] = [
            ("c", CALENDAR_ID),
            ("date", today),
            ("days", DAYS_WINDOW),
        ]
        for aud in AUDIENCE_IDS:
            base_params.append(("audience[]", aud))

        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://librarycalendar.fairfaxcounty.gov/calendar",
        }

        records: list[dict[str, Any]] = []
        offset = 0

        for _ in range(MAX_PAGES):
            params = base_params + [("offset", offset)]
            try:
                response = self.get(LIBCAL_URL, params=params, headers=headers)
                data = response.json()
            except Exception as exc:
                logger.warning("LibCal fetch failed at offset %d: %s", offset, exc)
                break

            results = data.get("results") or []
            if not results:
                break

            for event in results:
                records.append(self._map_event(event))

            total = data.get("total_results", 0)
            perpage = data.get("perpage", 20) or 20
            offset += perpage
            if offset >= total:
                break

        logger.debug("Fetched %d raw events from %s", len(records), self.source_name)
        return records

    def _map_event(self, event: dict[str, Any]) -> dict[str, Any]:
        campus = event.get("campus") or ""
        location = event.get("location") or ""
        location_text = ", ".join(p for p in [campus, location] if p) or None

        # LibCal occasionally populates a `cost` / `admission` / `fee` field on
        # paid programs. Pass any of these through as price_text so the
        # classifier sees an explicit signal instead of relying on the library
        # default "free" fallback.
        price_text: str | None = None
        for key in ("cost", "price", "fee", "admission", "registration_fee"):
            val = event.get(key)
            if val and isinstance(val, (str, int, float)):
                price_text = str(val).strip() or None
                if price_text:
                    break

        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_url": event.get("url") or "",
            "title": event.get("title") or "",
            "date_text": event.get("startdt"),       # "2026-03-16 10:00:00"
            "end_text": event.get("enddt"),
            "location_text": location_text,
            "summary_text": event.get("shortdesc") or event.get("description"),
            "price_text": price_text,
            "all_day": event.get("all_day", False),
            "online_event": event.get("online_event", False),
            "featured_image": event.get("featured_image"),
            "libcal_id": event.get("id"),
        }
