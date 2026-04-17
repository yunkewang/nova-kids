"""
Scraper for Fairfax County Park Authority events.

Source: https://www.fairfaxcounty.gov/parks/park-events-calendar
Type:   HTML (BeautifulSoup) — Drupal Views, server-side rendered

Pagination:  ?park-events-calendar=&page=N (0-indexed)
Event cards: div.events-list.views-row

Pricing: the calendar list cards almost never carry dollar amounts. The
actual "PRICE REGISTRATION $X.XX" block lives on the per-event detail page,
so this scraper always fetches the detail page when no pricing text could be
salvaged from the card description. Without the detail fetch, paid programs
reach normalize_record() with price_text=None and silently default to free.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from scrapers.detail_price import (
    extract_price_from_detail_html,
    fetch_detail_price,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.fairfaxcounty.gov"
EVENTS_URL = f"{BASE_URL}/parks/park-events-calendar"
MAX_PAGES = 10  # safety cap

# How many event detail pages to fetch per scraper run. Detail-page lookups
# add network cost, so cap them defensively. The Fairfax Parks calendar
# typically surfaces ~200 upcoming events, so this is effectively "all of
# them" with a safety margin.
DETAIL_FETCH_LIMIT = 500

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

# Pricing text we're willing to salvage from the Drupal card description.
# Covers "Cost: $20", "Registration fee: $15", "$10 per child", "Members free",
# "Free" / "Free event".
_PRICING_SNIPPET_RE = re.compile(
    r"(?:"
    r"(?:registration|program|class|course|entry|admission|materials?|ticket)"
    r"\s+fee[^.\n]{0,60}"
    r"|(?:cost|price|fee|admission)\s*[:\-]\s*[^.\n]{0,40}"
    r"|members?\s+free[^.\n]{0,40}"
    r"|\$\s*\d[\d,.]*(?:\s*(?:per\s+\w+|each|/\w+))?"
    r"|\bfree\s+(?:event|admission|for\s+\w+)\b"
    r"|\bsuggested\s+donation[^.\n]{0,40}"
    r")",
    re.IGNORECASE,
)


def _venue_from_url(url: str) -> str | None:
    """Extract a venue name hint from the Fairfax Parks URL path slug."""
    m = _PARK_SLUG_RE.search(url)
    if not m:
        return None
    slug = m.group(1)
    return _PARK_SLUG_TO_VENUE.get(slug)


def _extract_price_text(summary_text: str | None) -> str | None:
    """
    Pull a pricing snippet out of a card description.

    Many Fairfax Parks event cards bury pricing in the calendar description
    rather than in a dedicated field, e.g. "5:30PM, (ages 5-10) Nature walk.
    Registration fee: $12 per child." Without this extraction, the event would
    reach normalize_record() with price_text=None and be mis-classified as
    free by the source default.
    """
    if not summary_text:
        return None
    m = _PRICING_SNIPPET_RE.search(summary_text)
    if not m:
        return None
    return m.group(0).strip().strip(",.;:")


# Back-compat alias. Earlier tests imported _extract_price_from_detail_html
# from this module; the implementation has moved to scrapers.detail_price.
_extract_price_from_detail_html = extract_price_from_detail_html


class FairfaxParksAuthorityScraper(BaseScraper):
    """Scrapes public event listings from the Fairfax County Park Authority."""

    source_id = "fairfax_park_authority"
    source_name = "Fairfax County Park Authority"

    def __init__(self) -> None:
        super().__init__()
        # Track detail fetches across the run so we don't blow past the cap
        # if the calendar grows. Cleared automatically when the scraper is
        # re-instantiated for the next run.
        self._detail_fetches = 0

    def fetch_raw(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        self._detail_fetches = 0

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

        logger.info(
            "Fairfax Parks: parsed %d events, fetched %d detail pages",
            len(records), self._detail_fetches,
        )
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

        # 1. Try the card description first — cheap, avoids a round-trip.
        price_text = _extract_price_text(summary_text)

        # 2. Always fall back to the detail page when the card carries no
        #    pricing. Gating this on keyword hints (workshop/class/camp/...)
        #    was too narrow: events like "Trainers on the Go" (a paid
        #    Pokémon-Go hike) passed the keyword gate and silently defaulted
        #    to Free. The calendar list simply is not a reliable source of
        #    pricing signals, so we always consult the detail page when the
        #    card doesn't already answer the question.
        if not price_text:
            price_text = fetch_detail_price(
                self, event_url,
                fetch_count_attr="_detail_fetches",
                limit=DETAIL_FETCH_LIMIT,
            )

        return {
            "source_id":     self.source_id,
            "source_name":   self.source_name,
            "source_url":    event_url,
            "title":         title,
            "date_text":     date_text,
            "location_text": location_text,
            "summary_text":  summary_text,
            "price_text":    price_text,
        }
