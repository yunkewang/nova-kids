"""
Scraper for Arlington Public Library events.

Source: https://library.arlingtonva.us/events/
Type:   HTML (BeautifulSoup)

APL uses the LibCal system; this scraper targets children/family events.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://library.arlingtonva.us/events/"


class ArlingtonLibraryScraper(BaseScraper):
    """Scrapes children/family events from Arlington Public Library."""

    source_id = "arlington_public_library"
    source_name = "Arlington Public Library"

    def fetch_raw(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        url: str | None = BASE_URL

        while url:
            logger.debug("Fetching: %s", url)
            try:
                response = self.get(url)
            except Exception as exc:
                logger.warning("Failed to fetch %s: %s", url, exc)
                break

            soup = BeautifulSoup(response.text, "lxml")
            cards = soup.select(
                "div.s-lc-ea-evt, article.event-item, div.lc-event-card"
            )

            if not cards:
                logger.debug("No event cards found — stopping pagination.")
                break

            for card in cards:
                raw = self._parse_card(card)
                if raw:
                    records.append(raw)

            next_el = soup.select_one("a.next, a[aria-label='Next'], a[rel='next']")
            url = next_el["href"] if next_el else None

        return records

    def _parse_card(self, card: BeautifulSoup) -> dict[str, Any] | None:
        title_el = card.select_one("h3 a, h2 a, .s-lc-ea-evt-nm a, .event-title a")
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        event_url = href if href.startswith("http") else urljoin(BASE_URL, href)

        date_el = card.select_one(
            ".s-lc-ea-dt, .event-date, time, .lc-event-date"
        )
        date_text = date_el.get_text(strip=True) if date_el else None

        location_el = card.select_one(
            ".s-lc-ea-loc, .event-location, .lc-event-location"
        )
        location_text = location_el.get_text(strip=True) if location_el else None

        summary_el = card.select_one(".s-lc-ea-desc p, .event-description p")
        summary_text = summary_el.get_text(strip=True) if summary_el else None

        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_url": event_url,
            "title": title,
            "date_text": date_text,
            "location_text": location_text,
            "summary_text": summary_text,
        }
