"""
Shared helper for extracting event images from event *detail* pages.

Many list pages (Fairfax Parks, Arlington Parks & Rec) carry no image for
individual events, but the per-event DETAIL page usually declares one via
``<meta property="og:image">`` (or a Twitter-card fallback). This module
centralises that extraction so every scraper that already fetches detail
pages can attach an image without a second round-trip.

Scope is deliberately conservative: meta tags only, no <img> scraping.
Meta-tag images are almost always the event's own artwork, whereas generic
<img> harvesting on CMS pages frequently picks up logos, icons and ad
creatives.

Public API:
    extract_image_from_detail_html(html, base_url) -> str | None
    fetch_detail_image(scraper, url, fetch_count_attr="_detail_image_fetches",
                       limit=500) -> str | None
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from scrapers.detail_price import fetch_detail_html_text

logger = logging.getLogger(__name__)

# Safety net for standalone detail-page fetches; see fetch_detail_image.
DEFAULT_DETAIL_IMAGE_FETCH_LIMIT = 500

# Candidate meta tags in priority order. og:image is the standard; some
# pages only set the Twitter card tag or the secure-URL variant.
_IMAGE_META_TAGS: tuple[tuple[str, str], ...] = (
    ("property", "og:image"),
    ("name", "twitter:image"),
    ("property", "og:image:secure_url"),
)

# Filenames that are almost certainly site chrome rather than event
# artwork. Matched against the path basename (case-insensitive).
_SITE_CHROME_RE = re.compile(
    r"(?:favicon|logo|default-?og|og-?default|placeholder|spacer|pixel)",
    re.IGNORECASE,
)


def _is_usable_image_url(url: str) -> bool:
    """
    Reject obvious non-images: data: URIs, non-HTTP(S) schemes, and URLs
    that point at site chrome (favicons, logos, placeholder graphics).
    """
    if not url:
        return False
    lowered = url.strip().lower()
    if lowered.startswith("data:"):
        return False
    scheme = urlparse(url).scheme.lower()
    if scheme not in ("http", "https"):
        return False
    basename = urlparse(url).path.rsplit("/", 1)[-1]
    if _SITE_CHROME_RE.search(basename):
        return False
    return True


def extract_image_from_detail_html(
    html: str | None, base_url: str
) -> str | None:
    """
    Pull an event image URL out of an event detail-page HTML document.

    Strategy (first hit wins):
      1. <meta property="og:image">
      2. <meta name="twitter:image">
      3. <meta property="og:image:secure_url">

    Relative URLs are resolved against ``base_url`` (normally the detail
    page URL itself). ``data:`` URIs, non-HTTP(S) URLs and obvious site
    chrome (favicons, logos) are skipped rather than returned.
    """
    if not html:
        return None

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return None

    for attr, name in _IMAGE_META_TAGS:
        for tag in soup.find_all("meta", attrs={attr: name}):
            raw = tag.get("content")
            if not raw or not str(raw).strip():
                continue
            resolved = urljoin(base_url, str(raw).strip())
            if _is_usable_image_url(resolved):
                return resolved

    return None


def fetch_detail_image(
    scraper: Any,
    url: str,
    *,
    fetch_count_attr: str = "_detail_image_fetches",
    limit: int = DEFAULT_DETAIL_IMAGE_FETCH_LIMIT,
) -> str | None:
    """
    Fetch an event detail page and extract its image URL from the page's
    meta tags.

    The caller's scraper instance is expected to expose:
      - ``get(url)`` (inherited from BaseScraper) — polite HTTP GET
      - an integer counter attribute named ``fetch_count_attr`` (default
        ``_detail_image_fetches``) initialised to 0 per run

    The counter is incremented on every attempted fetch and capped at
    ``limit`` to bound per-run network cost.

    Returns the resolved image URL, or None when the fetch fails, the page
    is empty, or no usable image meta tag is present. Never raises.
    """
    html = fetch_detail_html_text(
        scraper, url, fetch_count_attr=fetch_count_attr, limit=limit
    )
    if html is None:
        return None
    return extract_image_from_detail_html(html, url)
