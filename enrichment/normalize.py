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
import html as _html_module
import logging
import re
from datetime import datetime, timezone
from typing import Any

from dateutil import parser as dateutil_parser
from pydantic import ValidationError

from config.schema import CostType, Event
from config.source_names import normalize_source_name
from enrichment.annotate import generate_short_note
from enrichment.enrich import enrich_event

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Virginia county/city keyword map used for county inference
# ---------------------------------------------------------------------------

_COUNTY_KEYWORDS: dict[str, list[str]] = {
    "Fairfax": [
        "fairfax", "reston", "herndon", "mclean", "annandale", "springfield",
        "centreville", "chantilly", "vienna", "falls church", "lorton",
        "oakton", "burke", "great falls", "clifton", "tyson", "mclean",
        "franconia", "hybla valley", "newington", "mount vernon",
    ],
    "Arlington": ["arlington"],
    "Loudoun": [
        "loudoun", "leesburg", "ashburn", "sterling", "lansdowne",
        "purcellville", "aldie", "dulles", "brambleton", "south riding",
        "lovettsville", "middleburg", "hamilton", "lucketts", "bluemont",
        "broadlands", "belmont ridge",
    ],
    "Prince William": [
        "prince william", "manassas", "woodbridge", "dale city",
        "dumfries", "triangle", "montclair", "bristow", "haymarket",
        "nokesville", "lake ridge", "independent hill",
    ],
    "Alexandria": ["alexandria"],
    # Note: DC venues are handled by known_venues.py / _KNOWN_VENUE_HINTS;
    # not inferred here to avoid false city-name matches.
}

# ---------------------------------------------------------------------------
# Title normalization
# ---------------------------------------------------------------------------

_LOWERCASE_WORDS = frozenset(
    {"a", "an", "the", "and", "but", "or", "for", "nor", "on", "at",
     "to", "by", "in", "of", "up", "as", "is", "it"}
)

# Titles that are clearly generic destination/venue/homepage titles,
# not event titles.  When detected, the title is kept but flagged.
_GENERIC_TITLE_PATTERNS = re.compile(
    r"^("
    r"facebook|twitter|instagram|linkedin"
    r"|parks\s*&?\s*recreation"
    r"|parks\s+and\s+recreation"
    r"|communications\s+and\s+community\s+engagement"
    r"|home\s*page?"
    r"|welcome\b"
    r"|untitled"
    r"|index"
    r"|coming\s*soon"
    r"|error|404|403|not\s*found|page\s*not\s*found"
    r")$",
    re.IGNORECASE,
)

# SEO keyword stuffing pattern: long title with em-dash followed by 3+ comma-
# separated terms ("Great Country Farms – Pick You Own, Strawberries, U-pick…")
_SEO_STUFFED_TITLE_RE = re.compile(r"\s*[–—]\s*.{0,60},.{0,60},")

# Sentence-style decorative tagline after em-dash: "Name – Verb. Verb. Verb."
# Matches patterns like "Children's Science Center – Explore. Create. Inspire"
_TAGLINE_DASH_RE = re.compile(r"\s*[–—]\s*[A-Z][a-z]+[.!]\s+[A-Z]")

# Geographic city/state qualifier appended to venue names
# e.g. "National Children's Museum of Washington Dc" → strip " of Washington Dc"
_GEO_QUALIFIER_RE = re.compile(
    r"\s+of\s+(?:Washington\s+D\.?C\.?|New\s+York\s+City|New\s+York|"
    r"[A-Z][a-zA-Z]+\s+[A-Z][a-zA-Z]+)\s*$",
    re.IGNORECASE,
)

# Site taglines that are clearly not specific event titles
_SITE_TAGLINE_RE = re.compile(
    r"(?:"
    r"arcade,?\s+sports\s+bar"
    r"|(?:art\s+classes|cooking\s+classes).{0,40}birthday\s+parties"
    r"|pick\s+you\s+own,.{0,20}u-?pick"
    r")",
    re.IGNORECASE,
)


def clean_event_title(title: str | None) -> str | None:
    """
    Remove common SEO / page-builder artifacts from extracted event titles.

    - Strips em-dash SEO suffix: "Venue – keyword1, keyword2, …" → "Venue"
    - Strips pipe site-name suffix: "Event | Site Name" → "Event"
    - Strips trailing punctuation
    - Returns None for known generic page titles
    """
    if not title or not title.strip():
        return None
    title = re.sub(r"\s+", " ", title).strip()

    # Detect purely generic page titles
    if _GENERIC_TITLE_PATTERNS.match(title):
        return None

    # Detect site taglines (not specific event descriptions)
    if _SITE_TAGLINE_RE.search(title):
        return None

    # Strip em-dash SEO keyword stuffing
    if _SEO_STUFFED_TITLE_RE.search(title):
        dash_pos = re.search(r"\s*[–—]", title)
        if dash_pos:
            title = title[: dash_pos.start()].strip()

    # Strip em-dash + sentence-style decorative tagline (e.g. "– Explore. Create.")
    if _TAGLINE_DASH_RE.search(title):
        dash_pos = re.search(r"\s*[–—]", title)
        if dash_pos:
            title = title[: dash_pos.start()].strip()

    # Strip geographic city/state qualifier appended to venue names
    title = _GEO_QUALIFIER_RE.sub("", title).strip()

    # Strip pipe site-name suffix ("Event | Brand")
    if " | " in title:
        title = title[: title.index(" | ")].strip()

    # Strip trailing period/comma (but keep "St.", "Jr.", etc.)
    title = re.sub(r"[.,]+$", "", title).strip()

    return title or None


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
            word = word.capitalize()
            # Fix "Mrs.b" → "Mrs. B" — single initial glued to abbreviation
            word = re.sub(r'(\.)([a-z])$', lambda m: m.group(1) + ' ' + m.group(2).upper(), word)
            result.append(word)
        else:
            result.append(lower)

    return " ".join(result)


# ---------------------------------------------------------------------------
# Summary cleaning
# ---------------------------------------------------------------------------

# WordPress/Visual Composer shortcodes — matches both complete [tag ...] and
# unclosed fragments that were truncated before the closing bracket.
_SHORTCODE_RE = re.compile(
    r"\[/?[a-z_][a-z0-9_]*(?:[^\]]*\]|[^\]]*$)",
    re.IGNORECASE | re.DOTALL,
)

# Very generic boilerplate summaries that carry no event-specific information
_GENERIC_SUMMARY_TEXTS = frozenset([
    "fairfax county, virginia",
    "fairfax county",
    "loudoun county",
    "arlington county",
    "city of alexandria",
    "arlington, virginia",
])

# Venue homepage / marketing blurb openers — clearly not event-specific summaries.
# Matched case-insensitively at the START of the cleaned summary text.
_VENUE_SUMMARY_BLURB_RE = re.compile(
    r"^(?:"
    # "Visit our / Explore our" — bookstore, paint-bar, venue pages
    r"visit\s+our\s+"
    r"|explore\s+our\s+"
    # "Our <X> school/center nurtures/offers" — Montessori, preschool blurbs
    r"|our\s+\w[\w\s]{2,20}\s+(?:school|center|studio)\s+\w"
    # Venue "offers" / "features" / "is a X" descriptions
    r"|the\s+\w[\w\s]{2,30}\s+(?:community\s+center|rec(?:reation)?\s+center|library)"
    r"\s+(?:offers|features|is\s+a\b)"
    r"|\w[\w\s]{2,30}\s+(?:recreation\s+center|community\s+center)\s+features"
    # Named venue "is dedicated to" / "features a" / "is a beautiful"
    r"|the\s+(?:claude\s+moore|alden\s+theatre?|charles\s+houston)\s+\w"
    r"|claude\s+moore\s+(?:rec(?:reation)?|park)\s+"
    r"|madison\s+features?\s+a\s+\w"
    r"|dulles\s+south\s+rec\s+and"
    r"|franklin\s+park\s+is\s+a\b"
    r"|port\s+discovery,?\s+located"
    r"|[a-z][\w\s'.-]{5,50}\s+is\s+a\s+beautiful\b"
    # Venue opening history ("X first opened its doors" / "Since YYYY, X")
    r"|since\s+\d{4},?\s+\w"
    r"|[a-z][\w\s]{2,40}\s+first\s+opened\s+its\s+doors"
    # Toy Nest homepage
    r"|a\s+toy\s+library\s+and\s+indoor\s+play"
    # AWLA / event registration CTA fragments
    r"|all\s+in\s+for\s+animals\b"
    r"|children\s+register\b"
    # URL-only summaries
    r"|learn\s+more\s+at\s+(?:www\.|https?://)"
    # Fairfax parks boilerplate
    r"|fairfax\s+county,?\s+virginia\s*-"
    # Stale webinar date lines ("Live Webinar May 16, 2024 …")
    r"|live\s+webinar\s+\w+\s+\d{1,2}"
    # Tackett's Mill homepage
    r"|tackett.?s\s+mill\s+center\s+is"
    # 501(c)3 nonprofit boilerplate ("X is a 501(c)3 non-profit organization")
    r"|.{5,60}\s+is\s+a\s+501\(c\)"
    # Generic venue "is a/an [adjective] facility" opener
    r"|[A-Za-z][\w\s']{3,50}\s+is\s+an?\s+(?:indoor|award-winning|vibrant|unique|community|premier|family-friendly)\s+"
    r")",
    re.IGNORECASE,
)


def clean_summary(text: str | None) -> str | None:
    """
    Strip junk from scraped summary text.

    - Removes WordPress/Visual Composer shortcodes
    - Decodes HTML entities (&amp; → &, &lt; → <, etc.)
    - Strips HTML tags
    - Collapses whitespace
    - Returns None when the result is too short or is known boilerplate
    """
    if not text:
        return None

    # Strip shortcodes
    text = _SHORTCODE_RE.sub(" ", text)

    # Decode HTML entities
    text = _html_module.unescape(text)

    # Strip HTML tags
    text = re.sub(r"<[^>]+>", " ", text)

    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) < 20:
        return None

    if text.lower().rstrip(".") in _GENERIC_SUMMARY_TEXTS:
        return None

    if _VENUE_SUMMARY_BLURB_RE.match(text):
        return None

    return text


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
        # Normalise to naive local time so all events sort consistently.
        if dt.tzinfo is not None:
            from datetime import timezone as _tz
            dt = dt.astimezone(_tz.utc).replace(tzinfo=None)
        return dt
    except (ValueError, OverflowError) as exc:
        logger.debug("Could not parse datetime %r: %s", text, exc)
        return None


# ---------------------------------------------------------------------------
# Location normalization
# ---------------------------------------------------------------------------

# ── Boilerplate cutoff patterns ─────────────────────────────────────────────
# When any of these appear, truncate the string at that point.
_LOC_BOILERPLATE_RE = re.compile(
    r"(?:"
    r"Get\s+Directions?"
    r"|Store\s+Hours?"
    r"|Phone\s+Number"
    r"|Connect\s+With\s+Us"
    r"|Hours\s+are\b"
    r"|Store\s+Features?"
    r"|FeaturesConnect"
    r"|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+\d{1,2}"   # "Sun 11-7"
    r")",
    re.IGNORECASE,
)

# Phone numbers anywhere in location text
_PHONE_RE = re.compile(r"\s*\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}")

# "Address" immediately followed by a capital letter or digit (B&N pattern).
# This means the scraper concatenated "Address" directly with the address text.
_ADDRESS_CONCAT_RE = re.compile(r"\bAddress([A-Z\d])")

# "• City, ST" or "• City, ST ZIPCODE" at end of string (DullesMoms venue fmt)
_BULLET_CITY_SUFFIX_RE = re.compile(
    r"\s*•\s*[A-Za-z][A-Za-z\s]+,\s*[A-Z]{2}(?:\s+\d{5})?\s*$"
)


def _clean_location_raw(text: str) -> tuple[str, str | None]:
    """
    First-pass cleanup of raw location text.

    Returns (cleaned_venue_text, extracted_address_or_None).

    Handles:
    - "Get Directions / Store Hours / Phone Number …" boilerplate
    - "Address[Capital]" concat artifact (Barnes & Noble)
    - Phone numbers embedded in location
    - "• City, ST" DullesMoms suffix
    - "• ParentOrg" bullet separators (keep only pre-bullet name)
    """
    # 0. Remove consecutively duplicated words ("Frying Pan Farm Farm" → "Frying Pan Farm")
    text = re.sub(r"\b(\w+)\s+\1\b", r"\1", text, flags=re.IGNORECASE)

    # 1. Cut at boilerplate markers
    m = _LOC_BOILERPLATE_RE.search(text)
    if m:
        text = text[: m.start()].strip()

    # 2. Remove phone numbers
    text = _PHONE_RE.sub("", text).strip()

    # 3. Handle "Address[Capital]" concat (B&N: "…VA Address20427 Exchange St…")
    extracted_addr: str | None = None
    m = _ADDRESS_CONCAT_RE.search(text)
    if m:
        addr_raw = text[m.start() + len("Address"):].strip()
        text = text[: m.start()].strip()
        extracted_addr = _normalize_address_spacing(addr_raw)

    # 4. Strip trailing punctuation (bullet split is deferred to normalize_location
    #    so that addresses embedded after "•" can still be found)
    text = text.rstrip(",:;").strip()

    return text, extracted_addr


def _strip_bullet_prefix(venue: str) -> str:
    """
    Remove '• ParentOrg' or '• City, ST' suffix from a venue name.

    Called AFTER any address has been extracted from the full text, so we
    don't accidentally discard address information still attached after "•".
    """
    # "• City, ST" at end
    venue = _BULLET_CITY_SUFFIX_RE.sub("", venue).strip()
    # Any remaining "• anything" — keep only the first segment
    if " • " in venue:
        venue = venue.split(" • ")[0].strip()
    return venue.rstrip(",:;").strip()


def _normalize_address_spacing(text: str) -> str:
    """
    Fix address formatting issues from scraped/concatenated text.

    - "20427 Exchange StAshburn"   → "20427 Exchange St, Ashburn"
    - "501 E. Pratt St.Baltimore"  → "501 E. Pratt St., Baltimore"
    - "SWWashington"               → "SW, Washington"
    - "22033Fairfax"               → "22033, Fairfax"
    """
    # Street abbreviation directly before capital city name ("StAshburn" → "St, Ashburn")
    # Note: no trailing \b because "StAshburn" has no word boundary between t and A
    text = re.sub(
        r"\b(St|Ave?|Rd|Dr|Blvd?|Ln|Ct|Pl|Way|Pkwy)([A-Z][a-z])",
        r"\1, \2", text,
    )
    # Abbreviation with period before capital ("St.Baltimore")
    text = re.sub(
        r"(\bSt|\bAve|\bRd|\bDr|\bBlvd|\bLn|\bCt|\bPl|SW|NW|SE|NE)\.([A-Z])",
        r"\1., \2", text,
    )
    # Compass direction run-together with city ("SWWashington")
    text = re.sub(r"\b(SW|NW|SE|NE)([A-Z][a-z])", r"\1, \2", text)
    # State abbreviation directly before ZIP (no space): "VA20147" → "VA 20147"
    text = re.sub(r"\b([A-Z]{2})(\d{5})\b", r"\1 \2", text)
    # 5-digit ZIP directly before a capital letter
    text = re.sub(r"(\d{5})([A-Z])", r"\1, \2", text)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _find_street_address_pos(text: str) -> int | None:
    """
    Return the character position where a street address begins in text,
    or None if no address is detected.

    Checks (in priority order):
      1. Comma + digits:          "…Venue, 501 Main St…"
      2. Two+ spaces + digits:    "…Venue  501 Main St…"
      3. Digit directly after text char (no separator):
                                  "…Museum650 Jefferson…"
      4. Single space + 3-5 digit number + capital word
                                  "…Venue 501 E. Pratt…"
    """
    # Pattern 1: comma before street number
    m = re.search(r",\s*\d{1,5}\s+[A-Z]", text)
    if m:
        return m.start()

    # Pattern 2: two or more spaces before street number
    m = re.search(r"\s{2,}\d{1,5}\s+[A-Z]", text)
    if m:
        return m.start()

    # Pattern 3: digit directly after lowercase (no separator — concatenation)
    m = re.search(r"(?<=[a-z])(\d{1,5})\s+[A-Z][a-zA-Z.]", text)
    if m:
        return m.start()

    # Pattern 4: single space + 3–5 digit street number + capital letter
    m = re.search(r"\s(\d{3,5})\s+[A-Z][a-zA-Z.]", text)
    if m:
        return m.start()

    return None


def _split_venue_address(text: str) -> tuple[str, str | None]:
    """
    Split "Venue Name, 123 Main St, City, VA 12345" into
    (venue_name, street_address_remainder).

    Returns (text, None) when no street address is detected.
    """
    pos = _find_street_address_pos(text)
    if pos is None:
        return text, None

    # Prefer splitting at the last comma before the street number
    split_pos = text.rfind(",", 0, pos)
    if split_pos == -1:
        split_pos = pos

    venue = text[:split_pos].strip().rstrip(",").strip()
    address = text[split_pos:].strip().lstrip(",").strip()

    if not venue:
        return text, None

    return venue, address


def normalize_location(location_text: str | None) -> dict[str, str | None]:
    """
    Return a dict with keys: location_name, location_address, city, county.

    Multi-step pipeline:
      1. Collapse whitespace
      2. Strip boilerplate (Get Directions, Store Hours, phone#, Address concat)
      3. Strip "• City, ST" DullesMoms suffix
      4. Split venue name from street address
      5. Infer city/county from keyword matching against full original text
    """
    if not location_text:
        return {
            "location_name": None,
            "location_address": None,
            "city": None,
            "county": None,
        }

    clean = re.sub(r"\s+", " ", location_text).strip()

    # Pre-clean: strip boilerplate and extract any embedded address (Address concat)
    clean, extracted_address = _clean_location_raw(clean)

    # Split venue name from inline street address BEFORE stripping bullet prefixes,
    # so addresses embedded after "•" (e.g. Udvar-Hazy / Smithsonian) are captured.
    venue_name, street_address = _split_venue_address(clean)

    # Now strip "• ParentOrg" / "• City, ST" from venue name
    venue_name = _strip_bullet_prefix(venue_name)

    # Prefer street_address from split; fall back to extracted_address from boilerplate
    final_address = street_address or extracted_address
    if final_address:
        final_address = _normalize_address_spacing(final_address)

    # Infer city/county from the full original text (before stripping)
    city: str | None = None
    county: str | None = None
    lower = location_text.lower()
    for cty, keywords in _COUNTY_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            county = cty
            for kw in keywords:
                if kw in lower:
                    city = kw.title()
                    break
            break

    return {
        "location_name": venue_name or None,
        "location_address": final_address,
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

    # Clean and normalize title
    cleaned_title = clean_event_title(title_raw)
    if cleaned_title is None:
        logger.warning("Skipping record with generic/unusable title: %r", title_raw)
        return None
    title = normalize_title(cleaned_title)

    source_url = normalize_url(raw.get("source_url")) or raw.get("source_url", "")
    raw_source_name = raw.get("source_name", raw.get("source_id", "unknown"))
    source_name = normalize_source_name(source_url, raw_source_name) or raw_source_name

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

    # Summary — clean junk before cost/note generation
    summary_text = clean_summary(raw.get("summary_text"))

    # Cost
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
        # Provenance — passed through from scraper / resolver
        "extracted_from": raw.get("extracted_from", "direct_scraper"),
        "extraction_confidence": float(raw.get("extraction_confidence", 1.0)),
        "short_note": None,  # filled in after enrichment
    }

    # Apply enrichment (tags, score, rainy_day, venue overrides)
    event_data = enrich_event(event_data)

    # Generate short_note from enriched facts
    event_data["short_note"] = generate_short_note(event_data)

    try:
        return Event(**event_data)
    except ValidationError as exc:
        logger.warning("Validation failed for '%s': %s", title, exc)
        return None
