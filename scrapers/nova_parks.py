"""
Scraper for NOVA Parks (Northern Virginia Regional Park Authority) events.

Source: https://www.novaparks.com/events
Type:   HTML (BeautifulSoup) — standard events listing

NOVA Parks (NVRPA) manages parks across Fairfax, Arlington, Loudoun, and
Prince William counties.  Their events page lists programs at parks such as
Bull Run, Lake Fairfax, Meadowlark Botanical Gardens, Algonkian Regional Park,
and more.

The scraper fetches the events listing page, extracts event cards, and for
each card follows the detail URL to obtain better location and time data.

Pagination is via ?page=N (1-indexed) or similar query patterns; we stop when
no new events are found or a max-page limit is reached.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL   = "https://www.novaparks.com"
EVENTS_URL = f"{BASE_URL}/events"
MAX_PAGES  = 8   # safety cap — NOVA Parks typically has <150 upcoming events

# Family/children relevance filter (applied to title + category)
_FAMILY_RE = re.compile(
    r"\b(?:family|families|child|children|kid|kids|toddler|preschool|baby|babies|"
    r"youth|storytime|story\s*time|little\s+ones|junior|elementary|teen|"
    r"sensory|nature\s+play|nature\s+walk|garden\s+walk|scavenger|"
    r"hike|hiking|camp|outdoor|wildflower|butterfly|animal|wildlife|"
    r"astronomy|star|constellation|train\s+ride|heritage|farm|harvest|"
    r"holiday|festival|fair|celebration|craft|arts?)\b",
    re.IGNORECASE,
)

# Explicit adult-only exclusions — skip if only these apply
_ADULT_ONLY_RE = re.compile(
    r"\b(?:adult only|21\+|beer|wine\s+tasting|spirits|cocktail|happy\s+hour|"
    r"corporate\s+event|wedding|networking|golf\s+tournament)\b",
    re.IGNORECASE,
)


def _is_family_relevant(title: str, description: str = "") -> bool:
    """Return True if the event is likely family/children-relevant."""
    combined = f"{title} {description}"
    if _ADULT_ONLY_RE.search(combined):
        return False
    # Accept nature, outdoor, and family events broadly — NOVA Parks is inherently
    # family-oriented; we accept most events and let the pipeline scoring rank them.
    if _FAMILY_RE.search(combined):
        return True
    # If none of the explicit keywords match, include cautiously — most NOVA Parks
    # programs are family-friendly by default (nature hikes, ecology programs, etc.)
    return True


def _make_page_url(page: int) -> str:
    """Build the events page URL for a given page number."""
    if page <= 1:
        return EVENTS_URL
    return f"{EVENTS_URL}?page={page}"


class NoVAParksScraper(BaseScraper):
    """
    Scrapes family and nature events from NOVA Parks (NVRPA) events calendar.

    Uses a two-pass approach:
      1. Fetch the listing page to discover event URLs and thumbnail data.
      2. For each event, use the listing-page data (title, date, location snippet).
         Full detail-page fetching is skipped to keep the scraper fast and polite.
    """

    source_id   = "nova_parks"
    source_name = "NOVA Parks"

    def fetch_raw(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for page in range(1, MAX_PAGES + 1):
            url = _make_page_url(page)
            logger.debug("NOVA Parks: fetching page %d: %s", page, url)

            try:
                resp = self.get(url)
            except Exception as exc:
                logger.warning("NOVA Parks: failed to fetch page %d: %s", page, exc)
                break

            soup = BeautifulSoup(resp.text, "lxml")
            page_records = self._parse_listing_page(soup, seen_urls)

            if not page_records:
                logger.debug("NOVA Parks: no new events on page %d — stopping.", page)
                break

            records.extend(page_records)

            # Stop if there is no next-page link
            if not self._has_next_page(soup):
                break

        # Adult/off-topic filter
        records = [r for r in records if _is_family_relevant(
            r.get("title", ""), r.get("summary_text") or ""
        )]

        logger.debug("Fetched %d events from %s", len(records), self.source_name)
        return records

    def _parse_listing_page(
        self, soup: BeautifulSoup, seen_urls: set[str]
    ) -> list[dict[str, Any]]:
        """Extract event records from one listing page."""
        records: list[dict[str, Any]] = []

        # NOVA Parks uses a variety of card/row layouts; we try several selectors.
        cards = (
            soup.select("article.event")
            or soup.select("div.event-item")
            or soup.select("div.views-row")
            or soup.select("li.event")
            or soup.select("div.node--type-event")
            or soup.select("div[class*='event']")
        )

        if not cards:
            # Fallback: look for any <article> or <div> that contains an event link
            cards = soup.select("article") or soup.select("div.views-row")

        for card in cards:
            raw = self._parse_card(card)
            if not raw:
                continue
            event_url = raw.get("source_url", "")
            if event_url and event_url in seen_urls:
                continue
            if event_url:
                seen_urls.add(event_url)
            records.append(raw)

        return records

    def _parse_card(self, card: BeautifulSoup) -> dict[str, Any] | None:
        """Parse a single event card from the listing page."""
        # Title and URL
        title_el = (
            card.select_one("h2 a")
            or card.select_one("h3 a")
            or card.select_one(".event-title a")
            or card.select_one(".title a")
            or card.select_one("a[href*='/events/']")
            or card.select_one("a[href*='/event/']")
        )
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        href  = title_el.get("href", "")
        if not title or not href:
            return None

        event_url = href if href.startswith("http") else urljoin(BASE_URL, href)

        # Skip non-event links (home, contact, etc.)
        if "/events/" not in event_url and "/event/" not in event_url:
            return None

        # Date/time
        date_el = (
            card.select_one("time")
            or card.select_one(".date-display-single")
            or card.select_one(".event-date")
            or card.select_one(".field--name-field-date")
            or card.select_one("[class*='date']")
        )
        date_text: str | None = None
        if date_el:
            date_text = (
                date_el.get("datetime")
                or date_el.get_text(separator=" ", strip=True)
            )

        # Location
        loc_el = (
            card.select_one(".event-location")
            or card.select_one(".location")
            or card.select_one(".field--name-field-location")
            or card.select_one("[class*='location']")
            or card.select_one("[class*='park']")
        )
        location_text: str | None = None
        if loc_el:
            location_text = loc_el.get_text(strip=True) or None

        # Summary / description snippet
        desc_el = (
            card.select_one(".event-description")
            or card.select_one(".field--name-body")
            or card.select_one(".views-field-body")
            or card.select_one("p")
        )
        summary_text: str | None = None
        if desc_el:
            summary_text = desc_el.get_text(separator=" ", strip=True) or None
            if summary_text and len(summary_text) > 400:
                summary_text = summary_text[:400]

        # Image
        img_el = card.select_one("img")
        image_url: str | None = None
        if img_el:
            src = img_el.get("src") or img_el.get("data-src") or ""
            if src:
                image_url = src if src.startswith("http") else urljoin(BASE_URL, src)

        return {
            "source_id":     self.source_id,
            "source_name":   self.source_name,
            "source_url":    event_url,
            "title":         title,
            "date_text":     date_text,
            "location_text": location_text,
            "summary_text":  summary_text,
            "image_url":     image_url,
        }

    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Return True if a 'next page' link exists on the listing page."""
        return bool(
            soup.select_one("a[rel='next']")
            or soup.select_one("a.pager__item--next")
            or soup.select_one("li.pager-next a")
            or soup.select_one(".next-page")
            or soup.select_one("a[title='Go to next page']")
        )
