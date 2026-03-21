"""
Scraper for Fairfax County Park Authority events.

Source: https://www.fairfaxcounty.gov/parks/park-events-calendar
Type:   HTML (BeautifulSoup) — Drupal Views, server-side rendered

Pagination:  ?park-events-calendar=&page=N (0-indexed)
Event cards: div.events-list.views-row
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.fairfaxcounty.gov"
EVENTS_URL = f"{BASE_URL}/parks/park-events-calendar"
MAX_PAGES = 10  # safety cap

# Map URL path slugs to canonical venue names used by geocode.py / known_venues.py
_PARK_SLUG_TO_VENUE: dict[str, str] = {
    "riverbend":          "Riverbend Park, Great Falls, VA",
    "eclawrence":         "Ellanor C. Lawrence Park, Chantilly, VA",
    "frying-pan-park":    "Frying Pan Farm Park, Herndon, VA",
    "burke-lake":         "Burke Lake Park, Burke, VA",
    "green-spring":       "Green Spring Gardens, Alexandria, VA",
    "lake-accotink":      "Lake Accotink Park, Springfield, VA",
    "huntley-meadows":    "Huntley Meadows Park, Alexandria, VA",
    "hidden-oaks":        "Hidden Oaks Nature Center, Annandale, VA",
    "hidden-pond":        "Hidden Pond Nature Center, Springfield, VA",
    "colvin-run-mill":    "Colvin Run Mill, Great Falls, VA",
    "turner-farm":        "Turner Farm, Great Falls, VA",
    "historic-huntley":   "Historic Huntley, Alexandria, VA",
    "sully-historic-site":"Sully Historic Site, Chantilly, VA",
    "reston-nature-center":"Reston Nature Center, Reston, VA",
    "meadowlark":         "Meadowlark Botanical Gardens, Vienna, VA",
    "oak-marr":           "Oak Marr Recreation Center, Oakton, VA",
    "spring-hill":        "Spring Hill Recreation Center, McLean, VA",
    "south-run":          "South Run Recreation Center, Springfield, VA",
    "cub-run":            "Cub Run Recreation Center, Chantilly, VA",
    "lee-district":       "Lee District Recreation Center, Springfield, VA",
    "audrey-moore":       "Audrey Moore Recreation Center, Annandale, VA",
    "franconia":          "Franconia Recreation Center, Springfield, VA",
}

_PARK_SLUG_RE = re.compile(r"/parks/([^/]+)/")


def _venue_from_url(url: str) -> str | None:
    """Extract a venue name hint from the Fairfax Parks URL path slug."""
    m = _PARK_SLUG_RE.search(url)
    if not m:
        return None
    slug = m.group(1)
    return _PARK_SLUG_TO_VENUE.get(slug)


class FairfaxParksAuthorityScraper(BaseScraper):
    """Scrapes public event listings from the Fairfax County Park Authority."""

    source_id = "fairfax_park_authority"
    source_name = "Fairfax County Park Authority"

    def fetch_raw(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []

        for page in range(MAX_PAGES):
            url = EVENTS_URL if page == 0 else f"{EVENTS_URL}?park-events-calendar=&page={page}"
            logger.debug("Fetching page %d: %s", page, url)

            try:
                response = self.get(url)
            except Exception as exc:
                logger.warning("Failed to fetch page %d: %s", page, exc)
                break

            soup = BeautifulSoup(response.text, "lxml")
            cards = soup.select("div.events-list.views-row")

            if not cards:
                logger.debug("No event cards on page %d — stopping.", page)
                break

            for card in cards:
                raw = self._parse_card(card)
                if raw:
                    records.append(raw)

            if not soup.select_one("a[rel='next']"):
                break

        return records

    def _parse_card(self, card: BeautifulSoup) -> dict[str, Any] | None:
        title_el = card.select_one("div.calendar-title a")
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        event_url = href if href.startswith("http") else urljoin(BASE_URL, href)

        # Date: <div class="date">Mar<br/>18</div>
        date_el = card.select_one("div.date")
        date_text: str | None = None
        if date_el:
            # Normalize: remove <br> and collapse whitespace
            date_text = date_el.get_text(separator=" ", strip=True)

        # Description: "5:30PM, (ages) Short description…"
        desc_el = card.select_one("div.calendar-description")
        summary_text = desc_el.get_text(strip=True) if desc_el else None

        # Derive venue from URL slug — the list view has no dedicated location element
        location_text = _venue_from_url(event_url)

        return {
            "source_id":     self.source_id,
            "source_name":   self.source_name,
            "source_url":    event_url,
            "title":         title,
            "date_text":     date_text,
            "location_text": location_text,
            "summary_text":  summary_text,
        }
