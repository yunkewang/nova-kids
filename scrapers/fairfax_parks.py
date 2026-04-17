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

# How many event detail pages to fetch per scraper run. Detail-page lookups
# add network cost, so cap them. The list page is still always parsed.
DETAIL_FETCH_LIMIT = 200

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

# Detail-page pricing label. Fairfax Parks renders a labelled block like:
#   PRICE
#   REGISTRATION  $115.00
# We grab the price token directly so the raw "$115.00" survives even when the
# label uses unusual whitespace or all-caps.
_DETAIL_PRICE_LABEL_RE = re.compile(
    r"\b(?:price|cost|fee|registration\s+fee|program\s+fee)\b[^\$\n\r]{0,80}"
    r"(\$\s*\d[\d,]*(?:\.\d+)?(?:\s*[-/]\s*\$?\d[\d,]*(?:\.\d+)?)?)",
    re.IGNORECASE,
)
_DETAIL_REGISTRATION_PRICE_RE = re.compile(
    r"\bregistration\b[^\$\n\r]{0,40}(\$\s*\d[\d,]*(?:\.\d+)?)",
    re.IGNORECASE,
)
# Stand-alone dollar amount as a fallback
_DETAIL_DOLLAR_RE = re.compile(r"\$\s*\d[\d,]*(?:\.\d+)?")

# Words in a title or summary that strongly suggest paid programming. Used to
# decide whether a detail-page fetch is worth the network round-trip.
_PAID_PROGRAM_TITLE_RE = re.compile(
    r"\b("
    r"workshop|class|classes|camp|camps|course|courses|lesson|lessons|series|"
    r"academy|clinic|clinics|training|certification|instruction|instructor|"
    r"intensive|seminar|tour|tours|excursion|trip|outing|"
    r"painting|drawing|pottery|ceramics|knitting|sewing|crochet|"
    r"yoga|pilates|dance|cooking|baking|swim(?:ming)?|tennis|golf|riding|"
    r"birthday|party|registration\s+required"
    r")\b",
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


def _extract_price_from_detail_html(html: str) -> str | None:
    """
    Pull a pricing snippet from a Fairfax Parks event detail page.

    The detail pages (e.g. an art class) render a structured block:

        PRICE
        REGISTRATION  $115.00

    The list calendar never carries that field, so without fetching the detail
    page these events arrive at the classifier with no signals and fall back
    to the parks "default free" rule. This helper salvages the labelled price
    directly from the page text.

    Strategy (first hit wins):
      1. Parse the page with BeautifulSoup, look for a labelled price block
      2. Look for "REGISTRATION $X.XX" pattern even without "PRICE" label
      3. Fall back to the first dollar amount in the visible body text
      4. Return None if nothing pricing-shaped was found
    """
    if not html:
        return None

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return None

    # Strip script/style noise so we don't grab CSS or analytics tokens
    for el in soup(["script", "style", "noscript"]):
        el.decompose()

    body_text = soup.get_text(" ", strip=True)
    body_text = re.sub(r"\s+", " ", body_text)
    if not body_text:
        return None

    # 1. Labelled price (PRICE / COST / FEE / REGISTRATION FEE) → "$X"
    m = _DETAIL_PRICE_LABEL_RE.search(body_text)
    if m:
        amount = m.group(1).strip()
        return f"Registration fee: {amount}" if "registration" in m.group(0).lower() \
            else f"Cost: {amount}"

    # 2. "REGISTRATION  $115.00" — common Fairfax Parks layout where the
    #    "PRICE" header appears on a sibling row that BeautifulSoup may
    #    flatten into a different sentence.
    m2 = _DETAIL_REGISTRATION_PRICE_RE.search(body_text)
    if m2:
        return f"Registration fee: {m2.group(1).strip()}"

    # 3. Free admission language is sometimes present
    if re.search(r"\bfree\s+(?:event|admission|to\s+attend|of\s+charge)\b",
                 body_text, re.IGNORECASE):
        return "Free admission"

    # 4. Last resort: any dollar amount on the page
    m3 = _DETAIL_DOLLAR_RE.search(body_text)
    if m3:
        return m3.group(0).strip()

    return None


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

        # Scan the card description for any pricing text. Passing this through
        # gives the classifier an explicit signal so paid parks programs aren't
        # collapsed into the "public source → free" default.
        price_text = _extract_price_text(summary_text)

        # The list card almost never carries pricing for paid programs (the
        # actual "PRICE REGISTRATION $X" block lives on the detail page).
        # Fetch the detail page when we still have no price and the title or
        # description hints at a paid-program format.
        if not price_text and self._should_fetch_detail(title, summary_text):
            price_text = self._fetch_detail_price(event_url)

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

    # ------------------------------------------------------------------
    # Detail-page helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _should_fetch_detail(title: str | None, summary: str | None) -> bool:
        """
        True when the event looks like it could be a paid program.

        We only fetch the detail page in cases where guessing wrong would be
        costly — i.e. titles/descriptions matching workshop/class/camp/course
        and similar paid-program keywords. Generic ranger walks and free
        nature talks don't trigger a detail fetch, so the network cost stays
        proportional to the number of paid-looking events.
        """
        haystack = " ".join(filter(None, (title, summary))).lower()
        if not haystack:
            return False
        return bool(_PAID_PROGRAM_TITLE_RE.search(haystack))

    def _fetch_detail_price(self, event_url: str) -> str | None:
        """Fetch a single event detail page and salvage pricing from it."""
        if self._detail_fetches >= DETAIL_FETCH_LIMIT:
            logger.debug(
                "Skipping detail fetch for %s — limit %d reached",
                event_url, DETAIL_FETCH_LIMIT,
            )
            return None
        self._detail_fetches += 1

        try:
            response = self.get(event_url)
        except Exception as exc:
            logger.debug("Detail fetch failed for %s: %s", event_url, exc)
            return None

        return _extract_price_from_detail_html(response.text)
