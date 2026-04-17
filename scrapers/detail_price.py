"""
Shared helper for extracting pricing text from event *detail* pages.

Most public parks calendars (Fairfax Parks, Arlington Parks & Rec, NOVA Parks)
render a calendar LIST page that does not carry pricing. The price only
appears on the per-event DETAIL page, in a block that looks like:

    PRICE
    REGISTRATION    $12.00

Without fetching the detail page, a $12 program silently ends up classified
as Free because no pricing signal reaches the classifier. This module
centralises detail-page fetching and price-string extraction so every parks
scraper can use it consistently.

Public API:
    extract_price_from_detail_html(html) -> str | None
    fetch_detail_price(scraper, url, fetch_count_attr="_detail_fetches",
                       limit=500) -> str | None
"""

from __future__ import annotations

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# How many detail-page fetches a single scraper run may perform. Applied as
# a safety net — a malformed list page must not trigger thousands of detail
# GETs.
DEFAULT_DETAIL_FETCH_LIMIT = 500


# ---------------------------------------------------------------------------
# Regexes — run against the flattened body text of a detail page
# ---------------------------------------------------------------------------

# "PRICE …$12.00", "Cost: $45", "Registration fee $80", "Program fee – $25"
_LABELLED_PRICE_RE = re.compile(
    r"\b(price|cost|fee|registration\s+fee|program\s+fee|class\s+fee|"
    r"course\s+fee|materials?\s+fee|admission|tuition)\b"
    r"[^\$\n\r]{0,120}"
    r"(\$\s*\d[\d,]*(?:\.\d+)?(?:\s*(?:[-/]|to)\s*\$?\d[\d,]*(?:\.\d+)?)?)",
    re.IGNORECASE,
)

# "REGISTRATION $115.00" — label without the preceding "PRICE"
_REGISTRATION_DOLLAR_RE = re.compile(
    r"\bregistration\b[^\$\n\r]{0,60}(\$\s*\d[\d,]*(?:\.\d+)?)",
    re.IGNORECASE,
)

# "$115.00 per child", "$45 per person"
_DOLLAR_PER_UNIT_RE = re.compile(
    r"(\$\s*\d[\d,]*(?:\.\d+)?)(\s*(?:per\s+\w+|/\s*\w+|each))?",
    re.IGNORECASE,
)

_FREE_TEXT_RE = re.compile(
    r"\b(free\s+admission|free\s+event|free\s+to\s+attend|free\s+of\s+charge|"
    r"no\s+charge|no\s+cost|no\s+fee|admission\s+is\s+free)\b",
    re.IGNORECASE,
)

# CSS selectors that commonly hold structured pricing on CMS-driven event
# pages. Tried in order; first non-empty text wins.
_PRICE_CSS_SELECTORS: tuple[str, ...] = (
    ".field--name-field-price",
    ".field--name-field-cost",
    ".field--name-field-fee",
    ".field--name-field-registration-fee",
    ".event-price",
    ".event-cost",
    ".event-fee",
    ".tribe-events-cost",
    ".price",
    ".cost",
    ".fee",
    "[class*='price']",
    "[class*='cost']",
    "[class*='fee']",
    "[itemprop='price']",
    "[data-price]",
    "dl.event-details dd",  # generic definition-list patterns
)


def _collect_body_text(soup: BeautifulSoup) -> str:
    """Flatten the page body to a single cleaned-up text string."""
    for el in soup(["script", "style", "noscript"]):
        el.decompose()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)


def _price_looks_nontrivial(text: str) -> bool:
    """True if *text* contains a dollar sign or an explicit free-text token."""
    if not text:
        return False
    if "$" in text:
        return True
    return bool(_FREE_TEXT_RE.search(text))


def extract_price_from_detail_html(html: str | None) -> str | None:
    """
    Pull a pricing snippet out of an event detail-page HTML document.

    Strategy (first hit wins):
      1. Dedicated pricing CSS containers (field--name-field-price, .event-cost, ...)
      2. Labelled pricing text in the flattened body ("PRICE ... $X", "Cost: $Y")
      3. "REGISTRATION $X" (label without "PRICE")
      4. Explicit free-admission language
      5. Bare "$X" fallback
    """
    if not html:
        return None

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return None

    # 1. Structured HTML containers
    for selector in _PRICE_CSS_SELECTORS:
        try:
            el = soup.select_one(selector)
        except Exception:
            continue
        if not el:
            continue
        txt = el.get_text(" ", strip=True)
        txt = re.sub(r"\s+", " ", txt)
        if _price_looks_nontrivial(txt):
            return txt[:200]

    body_text = _collect_body_text(soup)
    if not body_text:
        return None

    # 2. Labelled price token in the body text
    m = _LABELLED_PRICE_RE.search(body_text)
    if m:
        label = m.group(1).lower()
        amount = m.group(2).strip()
        if "registration" in label:
            return f"Registration fee: {amount}"
        if "program" in label or "class" in label or "course" in label:
            return f"{m.group(1).strip().title()}: {amount}"
        if "material" in label:
            return f"Materials fee: {amount}"
        if "admission" in label:
            return f"Admission: {amount}"
        if "tuition" in label:
            return f"Tuition: {amount}"
        return f"Cost: {amount}"

    # 3. "REGISTRATION $X" without a "PRICE" prefix — common on pages where
    #    the PRICE label is in a separate DOM node that BeautifulSoup flattens
    #    with unexpected whitespace.
    m2 = _REGISTRATION_DOLLAR_RE.search(body_text)
    if m2:
        return f"Registration fee: {m2.group(1).strip()}"

    # 4. Explicit free-admission language
    free_match = _FREE_TEXT_RE.search(body_text)
    if free_match:
        return free_match.group(0).strip()

    # 5. First bare dollar amount
    m3 = _DOLLAR_PER_UNIT_RE.search(body_text)
    if m3:
        snippet = (m3.group(1) + (m3.group(2) or "")).strip()
        return snippet or None

    return None


def fetch_detail_price(
    scraper: Any,
    url: str,
    *,
    fetch_count_attr: str = "_detail_fetches",
    limit: int = DEFAULT_DETAIL_FETCH_LIMIT,
) -> str | None:
    """
    Fetch an event detail page and extract pricing from it.

    The caller's scraper instance is expected to expose:
      - ``get(url)`` (inherited from BaseScraper) — polite HTTP GET
      - an integer counter attribute named ``fetch_count_attr`` (default
        ``_detail_fetches``) initialised to 0 per run

    The counter is incremented on every attempted fetch and capped at
    ``limit`` to bound per-run network cost.

    Returns the extracted pricing snippet, or None when the fetch fails,
    the page is empty, or no pricing-shaped text is present.
    """
    count = int(getattr(scraper, fetch_count_attr, 0) or 0)
    if count >= limit:
        logger.debug("Skipping detail fetch for %s — limit %d reached", url, limit)
        return None
    setattr(scraper, fetch_count_attr, count + 1)

    try:
        response = scraper.get(url)
    except Exception as exc:
        logger.debug("Detail fetch failed for %s: %s", url, exc)
        return None

    return extract_price_from_detail_html(response.text)
