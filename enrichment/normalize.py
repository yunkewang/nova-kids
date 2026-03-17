"""
Normalization layer — converts raw scraper dicts into Event objects.

Each raw dict produced by a scraper is passed through:
  1. normalize_title()         — clean up title casing and whitespace
  2. parse_datetime()          — turn date/time strings into datetime objects
  3. normalize_location()      — clean address, infer city/county
  4. normalize_cost()          — detect free vs. paid
  5. normalize_url()           — ensure URLs are well-formed
  6. build_event()             — assemble the final Event, calling enrichment

The public entry point is `normalize_record(raw: dict) -> Event | None`.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any

from dateutil import parser as dateutil_parser
from pydantic import ValidationError

from config.schema import CostType, Event
from enrichment.enrich import enrich_event

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Virginia county/city keyword map used for county inference
# ---------------------------------------------------------------------------

_COUNTY_KEYWORDS: dict[str, list[str]] = {
    "Fairfax": ["fairfax", "reston", "herndon", "mclean", "annandale", "springfield",
                "centreville", "chantilly", "vienna", "falls church"],
    "Arlington": ["arlington"],
    "Loudoun": ["loudoun", "leesburg", "ashburn", "sterling", "lansdowne"],
    "Prince William": ["prince william", "manassas", "woodbridge", "dale city"],
    "Alexandria": ["alexandria"],
}

# ---------------------------------------------------------------------------
# Title normalization
# ---------------------------------------------------------------------------

# Words that should stay lowercase in title case (unless at start)
_LOWERCASE_WORDS = frozenset(
    {"a", "an", "the", "and", "but", "or", "for", "nor", "on", "at",
     "to", "by", "in", "of", "up", "as", "is", "it"}
)


def normalize_title(title: str) -> str:
    """
    Convert a title to clean title case, collapsing extra whitespace.

    Examples:
        "FAMILY STORY TIME at the library" -> "Family Story Time at the Library"
    """
    if not title:
        return title

    title = re.sub(r"\s+", " ", title).strip()
    words = title.split()
    result: list[str] = []

    for i, word in enumerate(words):
        lower = word.lower()
        if i == 0 or lower not in _LOWERCASE_WORDS:
            result.append(word.capitalize())
        else:
            result.append(lower)

    return " ".join(result)


# ---------------------------------------------------------------------------
# Date / time parsing
# ---------------------------------------------------------------------------

def parse_datetime(
    text: str | None,
    *,
    default_year: int | None = None,
) -> datetime | None:
    """
    Parse a human-readable date/time string into a datetime.

    Returns None if parsing fails rather than raising.
    """
    if not text or not text.strip():
        return None

    text = text.strip()

    # Replace common separators that confuse dateutil
    text = re.sub(r"\bat\b", " ", text, flags=re.IGNORECASE)

    try:
        dt = dateutil_parser.parse(text, fuzzy=True)
        # If no year was in the string, default_year shifts the date forward
        if default_year and dt.year < default_year:
            dt = dt.replace(year=default_year)
        return dt
    except (ValueError, OverflowError) as exc:
        logger.debug("Could not parse datetime %r: %s", text, exc)
        return None


# ---------------------------------------------------------------------------
# Location normalization
# ---------------------------------------------------------------------------

def normalize_location(
    location_text: str | None,
) -> dict[str, str | None]:
    """
    Return a dict with keys: location_name, location_address, city, county.

    For now this does basic cleanup and keyword-based county inference.
    A future iteration can call a geocoding API.
    """
    if not location_text:
        return {
            "location_name": None,
            "location_address": None,
            "city": None,
            "county": None,
        }

    clean = re.sub(r"\s+", " ", location_text).strip()
    city: str | None = None
    county: str | None = None

    lower = clean.lower()
    for cty, keywords in _COUNTY_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            county = cty
            # Use the first matching keyword as a city hint
            for kw in keywords:
                if kw in lower:
                    city = kw.title()
                    break
            break

    return {
        "location_name": clean,
        "location_address": None,  # address parsing requires geocoding
        "city": city,
        "county": county,
    }


# ---------------------------------------------------------------------------
# Cost normalization
# ---------------------------------------------------------------------------

_FREE_PATTERNS = re.compile(
    r"\bfree\b|\bno charge\b|\bno cost\b|\bcomplimentary\b",
    re.IGNORECASE,
)
_PAID_PATTERNS = re.compile(
    r"\$\s*\d|\bregister\b|\bticket\b|\bfee\b|\bpaid\b|\bcost[s:]?\b",
    re.IGNORECASE,
)


def normalize_cost(
    price_text: str | None,
    summary: str | None = None,
) -> tuple[CostType, str | None]:
    """
    Infer CostType from price_text and/or summary.

    Returns (cost_type, cleaned_price_text).
    """
    combined = " ".join(filter(None, [price_text, summary]))
    if not combined:
        return CostType.UNKNOWN, None

    if _FREE_PATTERNS.search(combined):
        return CostType.FREE, price_text
    if _PAID_PATTERNS.search(combined):
        return CostType.PAID, price_text
    return CostType.UNKNOWN, price_text


# ---------------------------------------------------------------------------
# URL normalization
# ---------------------------------------------------------------------------

_URL_PATTERN = re.compile(
    r"^https?://[^\s/$.?#].[^\s]*$", re.IGNORECASE
)


def is_valid_url(url: str | None) -> bool:
    """Return True if url looks like a well-formed HTTP/HTTPS URL."""
    if not url:
        return False
    return bool(_URL_PATTERN.match(url.strip()))


def normalize_url(url: str | None) -> str | None:
    """Strip whitespace and return None if the URL is invalid."""
    if not url:
        return None
    url = url.strip()
    return url if is_valid_url(url) else None


# ---------------------------------------------------------------------------
# Stable ID generation
# ---------------------------------------------------------------------------

def generate_event_id(
    title: str,
    start: datetime,
    location_name: str | None,
    source_url: str,
) -> str:
    """
    Generate a stable, deterministic ID from key event fields.

    Using a short SHA-256 prefix keeps IDs compact and collision-resistant.
    """
    raw = "|".join(
        [
            title.lower().strip(),
            start.isoformat(),
            (location_name or "").lower().strip(),
            source_url.strip(),
        ]
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Top-level normalizer
# ---------------------------------------------------------------------------

def normalize_record(raw: dict[str, Any]) -> Event | None:
    """
    Convert a raw scraper dict to a validated, enriched Event.

    Returns None if the record cannot be normalized (logged as a warning).

    Expected raw keys (all optional except title/source_url/source_name):
        title, summary_text, date_text, start_text, end_text,
        location_text, price_text, source_url, source_name, source_id,
        registration_url, image_url
    """
    title_raw: str = raw.get("title", "")
    if not title_raw or not title_raw.strip():
        logger.warning("Skipping record with empty title: %r", raw)
        return None

    title = normalize_title(title_raw)
    source_url = normalize_url(raw.get("source_url")) or raw.get("source_url", "")
    source_name = raw.get("source_name", raw.get("source_id", "unknown"))

    # Parse datetimes
    current_year = datetime.now().year
    start_text = raw.get("start_text") or raw.get("date_text")
    end_text = raw.get("end_text")

    start = parse_datetime(start_text, default_year=current_year)
    if start is None:
        logger.warning("Skipping '%s' — could not parse start date from %r", title, start_text)
        return None

    end = parse_datetime(end_text, default_year=current_year)

    # Location
    loc = normalize_location(raw.get("location_text"))

    # Cost
    summary_text = raw.get("summary_text")
    cost_type, price_text = normalize_cost(raw.get("price_text"), summary_text)

    # Generate ID
    event_id = generate_event_id(title, start, loc["location_name"], source_url)

    # Assemble base event dict
    event_data: dict[str, Any] = {
        "id": event_id,
        "title": title,
        "summary": summary_text,
        "start": start,
        "end": end,
        "all_day": raw.get("all_day", False),
        "location_name": loc["location_name"],
        "location_address": loc["location_address"],
        "city": loc["city"],
        "county": loc["county"],
        "cost_type": cost_type,
        "price_text": price_text,
        "source_name": source_name,
        "source_url": source_url,
        "registration_url": normalize_url(raw.get("registration_url")),
        "image_url": normalize_url(raw.get("image_url")),
        "last_verified_at": datetime.now(tz=timezone.utc),
        "tags": [],
        "family_friendly_score": 0.0,
        "rainy_day_friendly": False,
    }

    # Apply enrichment (tags, score, rainy_day)
    event_data = enrich_event(event_data)

    try:
        return Event(**event_data)
    except ValidationError as exc:
        logger.warning("Validation failed for '%s': %s", title, exc)
        return None
