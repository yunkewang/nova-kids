"""
Scraper for Arlington County Parks & Recreation events.

Source: https://parks.arlingtonva.us/events/
Type:   HTML (BeautifulSoup)

Starter implementation — selectors must be verified against the live page.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://parks.arlingtonva.us/events/"


class ArlingtonParksRecScraper(BaseScraper):
    """Scrapes public event listings from Arlington County Parks & Recreation."""

    source_id = "arlington_parks_rec"
    source_name = "Arlington County Parks & Recreation"

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
            event_cards = soup.select(
                "article.tribe-events-calendar-list__event-article, "
                "div.tribe-event, "
                ".tribe-events-calendar-list__event"
            )

            if not event_cards:
                logger.debug("No event cards found — stopping pagination.")
                break

            for card in event_cards:
                raw = self._parse_card(card)
                if raw:
                    records.append(raw)

            # The Events Calendar plugin uses rel=next for pagination
            next_el = soup.select_one("a.tribe-events-c-nav__next, a[rel='next']")
            url = next_el["href"] if next_el else None

        return records

    def _parse_card(self, card: BeautifulSoup) -> dict[str, Any] | None:
        title_el = card.select_one(
            ".tribe-events-calendar-list__event-title a, h3 a, h2 a"
        )
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        event_url = href if href.startswith("http") else urljoin(BASE_URL, href)

        # The Events Calendar plugin uses <abbr> with machine-readable dates
        start_el = card.select_one("abbr.tribe-events-abbr--start, time.tribe-event-date-start")
        start_text = (
            start_el.get("title") or start_el.get_text(strip=True)
            if start_el
            else None
        )
        end_el = card.select_one("abbr.tribe-events-abbr--end, time.tribe-event-date-end")
        end_text = (
            end_el.get("title") or end_el.get_text(strip=True)
            if end_el
            else None
        )

        venue_el = card.select_one(".tribe-venue, .tribe-events-calendar-list__event-venue")
        location_text = venue_el.get_text(strip=True) if venue_el else None

        summary_el = card.select_one(".tribe-events-calendar-list__event-description p")
        summary_text = summary_el.get_text(strip=True) if summary_el else None

        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_url": event_url,
            "title": title,
            "start_text": start_text,
            "end_text": end_text,
            "location_text": location_text,
            "summary_text": summary_text,
        }
