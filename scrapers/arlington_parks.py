"""
Scraper for Arlington County Parks & Recreation events.

Source: https://www.arlingtonva.us/Government/Departments/Parks-Recreation/Parks-Events
Type:   HTML (BeautifulSoup) — Sitecore CMS, page 1 only

The old URL (parks.arlingtonva.us/events/) now redirects to a generic
department page.  The live events listing is at the URL above.

Pagination uses Sitecore's encrypted __SEAMLESSVIEWSTATE and cannot be
driven by simple GET requests — only the first page (~10 events) is scraped.

Akamai edge caching blocks generic browser User-Agents with 403;
the pipeline's own UA (NoVAKidsPipeline/1.0) is allowed through.
"""

from __future__ import annotations

import logging
from typing import Any

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from scrapers.detail_price import fetch_detail_price

logger = logging.getLogger(__name__)

EVENTS_URL = (
    "https://www.arlingtonva.us"
    "/Government/Departments/Parks-Recreation/Parks-Events"
)
BASE_URL = "https://www.arlingtonva.us"

# Arlington's Parks-Events page lists ~10 items, so fetching every detail
# page is cheap.
DETAIL_FETCH_LIMIT = 50


class ArlingtonParksRecScraper(BaseScraper):
    """Scrapes public event listings from Arlington County Parks & Recreation (page 1 only)."""

    source_id = "arlington_parks_rec"
    source_name = "Arlington County Parks & Recreation"

    def __init__(self) -> None:
        super().__init__()
        self._detail_fetches = 0

    def fetch_raw(self) -> list[dict[str, Any]]:
        self._detail_fetches = 0
        try:
            response = self.get(EVENTS_URL)
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", EVENTS_URL, exc)
            return []

        soup = BeautifulSoup(response.text, "lxml")
        cards = soup.select("div.list-item-container")

        if not cards:
            logger.warning(
                "No event cards found on %s — page structure may have changed.",
                EVENTS_URL,
            )
            return []

        records = []
        for card in cards:
            raw = self._parse_card(card)
            if raw:
                records.append(raw)

        logger.info(
            "Arlington Parks: parsed %d events, fetched %d detail pages",
            len(records), self._detail_fetches,
        )
        return records

    def _parse_card(self, card: BeautifulSoup) -> dict[str, Any] | None:
        link_el = card.select_one("a[href*='/Parks-Events/']")
        if not link_el:
            return None

        href = link_el.get("href", "")
        event_url = href if href.startswith("http") else f"{BASE_URL}{href}"

        title_el = card.select_one("h2.list-item-title")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)

        # Date parts rendered as separate spans, e.g.:
        # <span class="part-month">Mar</span>
        # <span class="part-date">18</span>
        # <span class="part-year">2026</span>
        month_el = card.select_one("span.part-month, .part-month")
        day_el = card.select_one("span.part-date, .part-date")
        year_el = card.select_one("span.part-year, .part-year")
        date_parts = [
            el.get_text(strip=True)
            for el in [month_el, day_el, year_el]
            if el
        ]
        date_text = " ".join(date_parts) if date_parts else None

        desc_el = card.select_one("span.list-item-block-desc, .list-item-block-desc")
        summary_text = desc_el.get_text(strip=True) if desc_el else None

        # The Arlington list card has no pricing field; the per-event page
        # carries "$X" or "Free" in a body block. Always fetch the detail
        # page so paid programs don't silently default to free.
        price_text = fetch_detail_price(
            self, event_url,
            fetch_count_attr="_detail_fetches",
            limit=DETAIL_FETCH_LIMIT,
        )

        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_url": event_url,
            "title": title,
            "date_text": date_text,
            "location_text": None,
            "summary_text": summary_text,
            "price_text": price_text,
        }
