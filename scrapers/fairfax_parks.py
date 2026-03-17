"""
Scraper for Fairfax County Park Authority events.

Source: https://www.fairfaxcounty.gov/parks/park-events
Type:   HTML (BeautifulSoup)

This is a *starter* implementation.  The actual CSS selectors will need to be
verified and updated against the live page structure.  The scraper is kept
intentionally simple — it returns raw dicts; normalization happens elsewhere.
"""

from __future__ import annotations

import logging
from typing import Any

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.fairfaxcounty.gov/parks/park-events"


class FairfaxParksAuthorityScraper(BaseScraper):
    """Scrapes public event listings from the Fairfax County Park Authority."""

    source_id = "fairfax_park_authority"
    source_name = "Fairfax County Park Authority"

    def fetch_raw(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page = 0

        while True:
            url = BASE_URL if page == 0 else f"{BASE_URL}?page={page}"
            logger.debug("Fetching page %d: %s", page, url)

            try:
                response = self.get(url)
            except Exception as exc:
                logger.warning("Failed to fetch page %d: %s", page, exc)
                break

            soup = BeautifulSoup(response.text, "lxml")
            event_cards = soup.select("div.views-row, article.event-item")

            if not event_cards:
                logger.debug("No event cards found on page %d — stopping.", page)
                break

            for card in event_cards:
                raw = self._parse_card(card, base_url=BASE_URL)
                if raw:
                    records.append(raw)

            # Check for a "next page" link; stop when absent
            next_link = soup.select_one("a[rel='next'], li.pager-next a")
            if not next_link:
                break
            page += 1

        return records

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_card(
        self, card: BeautifulSoup, base_url: str
    ) -> dict[str, Any] | None:
        """
        Extract raw fields from a single event card element.

        Returns None if the card does not look like a valid event.
        These selectors are approximate — update them after inspecting
        the live page with browser devtools or `curl | htmlq`.
        """
        title_el = card.select_one("h3 a, h2 a, .field--name-title a")
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        event_url = href if href.startswith("http") else f"https://www.fairfaxcounty.gov{href}"

        date_el = card.select_one(".date-display-single, time, .field--name-field-date")
        date_text = date_el.get_text(strip=True) if date_el else None

        location_el = card.select_one(".field--name-field-location, .event-location")
        location_text = location_el.get_text(strip=True) if location_el else None

        summary_el = card.select_one(".field--name-body p, .event-summary")
        summary_text = summary_el.get_text(strip=True) if summary_el else None

        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_url": event_url,
            "title": title,
            "date_text": date_text,
            "location_text": location_text,
            "summary_text": summary_text,
            "raw_html_snippet": str(card)[:500],  # keep for debugging
        }
