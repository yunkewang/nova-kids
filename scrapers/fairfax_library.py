"""
Scraper for Fairfax County Public Library events.

Source: https://www.fairfaxcounty.gov/library/programs-events
Type:   HTML (BeautifulSoup)

FCPL lists events for all 23 branches on a single page with filters.
This scraper targets the family/children category.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.fairfaxcounty.gov/library/programs-events"
# Filter for children and family programs
EVENTS_URL = BASE_URL + "?category=children"


class FairfaxLibraryScraper(BaseScraper):
    """Scrapes children/family events from Fairfax County Public Library."""

    source_id = "fairfax_county_library"
    source_name = "Fairfax County Public Library"

    def fetch_raw(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page = 0

        while True:
            url = EVENTS_URL if page == 0 else f"{EVENTS_URL}&page={page}"
            logger.debug("Fetching page %d: %s", page, url)

            try:
                response = self.get(url)
            except Exception as exc:
                logger.warning("Failed to fetch page %d: %s", page, exc)
                break

            soup = BeautifulSoup(response.text, "lxml")
            rows = soup.select(
                "div.views-row, article.event-teaser, li.event-listing"
            )

            if not rows:
                logger.debug("No event rows on page %d — stopping.", page)
                break

            for row in rows:
                raw = self._parse_row(row)
                if raw:
                    records.append(raw)

            next_el = soup.select_one("a[rel='next'], li.pager-next a")
            if not next_el:
                break
            page += 1

        return records

    def _parse_row(self, row: BeautifulSoup) -> dict[str, Any] | None:
        title_el = row.select_one("h3 a, h2 a, .field--name-title a, .event-title a")
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        event_url = href if href.startswith("http") else urljoin(BASE_URL, href)

        date_el = row.select_one(
            ".field--name-field-date, .date-display-single, time"
        )
        date_text = date_el.get_text(strip=True) if date_el else None

        branch_el = row.select_one(
            ".field--name-field-location, .library-branch, .event-location"
        )
        location_text = branch_el.get_text(strip=True) if branch_el else None

        summary_el = row.select_one(".field--name-body p, .event-description p")
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
