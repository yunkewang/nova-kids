"""
Original-source resolver.

Given a CandidateEvent with a candidate_original_url, this module:
  1. Fetches the original event page.
  2. Extracts structured facts using:
     a. schema.org/Event JSON-LD microdata (most reliable)
     b. Open Graph / meta tags (common fallback)
     c. Common HTML patterns (last resort)
  3. Returns a raw dict suitable for normalize_record() with provenance fields,
     or updates the candidate as requiring manual review if extraction fails.

This module only stores content from original host pages.
It must never read or republish DullesMoms page content.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from config.settings import (
    REQUEST_DELAY,
    REQUEST_MAX_RETRIES,
    REQUEST_TIMEOUT,
    SEED_CONFIDENCE_THRESHOLD,
    USER_AGENT,
)
from models.candidate import CandidateEvent, CandidateStatus

logger = logging.getLogger(__name__)

# Minimum confidence for extracted facts to be publishable automatically
_MIN_PUBLISH_CONFIDENCE = SEED_CONFIDENCE_THRESHOLD


# ---------------------------------------------------------------------------
# HTTP helper (single shared session per resolver call)
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(
        total=REQUEST_MAX_RETRIES,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ---------------------------------------------------------------------------
# Extraction strategies
# ---------------------------------------------------------------------------

def _extract_jsonld(soup: BeautifulSoup) -> dict[str, Any]:
    """
    Extract event facts from schema.org/Event JSON-LD blocks.

    Returns a dict with any of: title, start, end, location_name,
    location_address, cost_text, description_snippet, registration_url.
    """
    facts: dict[str, Any] = {}

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue

        # Handle both single object and @graph arrays
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            # Support @graph wrapper
            if item.get("@type") == "ItemList" or "@graph" in item:
                items.extend(item.get("@graph", []) if isinstance(item.get("@graph"), list) else [])
                continue

            schema_type = item.get("@type", "")
            if "Event" not in str(schema_type):
                continue

            if "name" in item and not facts.get("title"):
                facts["title"] = str(item["name"]).strip()

            if "startDate" in item and not facts.get("start_text"):
                facts["start_text"] = str(item["startDate"])

            if "endDate" in item and not facts.get("end_text"):
                facts["end_text"] = str(item["endDate"])

            # Location
            location = item.get("location", {})
            if isinstance(location, dict):
                if "name" in location and not facts.get("location_name"):
                    facts["location_name"] = str(location["name"]).strip()
                address = location.get("address", {})
                if isinstance(address, dict):
                    parts = [
                        address.get("streetAddress", ""),
                        address.get("addressLocality", ""),
                        address.get("addressRegion", ""),
                        address.get("postalCode", ""),
                    ]
                    addr_str = ", ".join(p for p in parts if p)
                    if addr_str and not facts.get("location_address"):
                        facts["location_address"] = addr_str
                elif isinstance(address, str) and not facts.get("location_address"):
                    facts["location_address"] = address

            # Offers / pricing
            offers = item.get("offers", {})
            if isinstance(offers, dict):
                price = offers.get("price", "")
                currency = offers.get("priceCurrency", "")
                if price is not None and not facts.get("cost_text"):
                    facts["cost_text"] = f"{currency} {price}".strip() if currency else str(price)
            elif isinstance(offers, list) and offers and not facts.get("cost_text"):
                offer = offers[0]
                if isinstance(offer, dict):
                    price = offer.get("price", "")
                    facts["cost_text"] = str(price) if price is not None else None

            # Description (capped for snippet)
            if "description" in item and not facts.get("description_snippet"):
                desc = str(item["description"]).strip()
                facts["description_snippet"] = desc[:280] if desc else None

            # Registration / event URL
            event_url = item.get("url") or item.get("eventUrl")
            if event_url and not facts.get("registration_url"):
                facts["registration_url"] = str(event_url)

            # Organizer → source name hint
            organizer = item.get("organizer", {})
            if isinstance(organizer, dict) and not facts.get("source_name_hint"):
                facts["source_name_hint"] = organizer.get("name", "")

    return facts


def _extract_opengraph(soup: BeautifulSoup) -> dict[str, Any]:
    """Extract basic facts from Open Graph and standard meta tags."""
    facts: dict[str, Any] = {}

    def meta(prop: str) -> str | None:
        el = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if el and isinstance(el, dict.__class__):
            return None
        return el.get("content", "").strip() if el else None  # type: ignore[union-attr]

    if not facts.get("title"):
        og_title = meta("og:title")
        page_title_el = soup.find("title")
        page_title = page_title_el.get_text(strip=True) if page_title_el else None
        facts["title"] = og_title or page_title

    if not facts.get("description_snippet"):
        desc = meta("og:description") or meta("description")
        if desc:
            facts["description_snippet"] = desc[:280]

    if not facts.get("image_url"):
        facts["image_url"] = meta("og:image")

    return facts


def _extract_html_patterns(soup: BeautifulSoup) -> dict[str, Any]:
    """
    Last-resort HTML pattern extraction.

    Looks for common event page markup that isn't captured by JSON-LD or OG tags.
    Returns only what it can find; caller merges with higher-priority results.
    """
    facts: dict[str, Any] = {}

    # Date / time — look for <time> elements with datetime attributes
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el and not facts.get("start_text"):
        facts["start_text"] = time_el.get("datetime") or time_el.get_text(strip=True)

    # Venue / location
    for selector in (
        "[itemprop='location']",
        "[itemprop='name'][class*='venue']",
        ".venue-name, .event-venue, .location-name",
        "[class*='venue'] h2, [class*='venue'] h3",
    ):
        el = soup.select_one(selector)
        if el and not facts.get("location_name"):
            facts["location_name"] = el.get_text(strip=True)
            break

    # Address
    for selector in (
        "[itemprop='address']",
        ".venue-address, .event-address, address",
    ):
        el = soup.select_one(selector)
        if el and not facts.get("location_address"):
            facts["location_address"] = el.get_text(strip=True)
            break

    # Cost
    for selector in (".event-cost, .ticket-price, [class*='price'], [class*='cost']",):
        el = soup.select_one(selector)
        if el and not facts.get("cost_text"):
            facts["cost_text"] = el.get_text(strip=True)
            break

    return facts


def _merge_facts(*fact_dicts: dict[str, Any]) -> dict[str, Any]:
    """
    Merge multiple fact dicts, preferring earlier (higher-priority) values.

    Later dicts fill in only keys that are still missing (None or absent).
    """
    merged: dict[str, Any] = {}
    for d in fact_dicts:
        for k, v in d.items():
            if v and k not in merged:
                merged[k] = v
    return merged


def _find_original_url_from_detail_page(
    seed_url: str,
    session: requests.Session,
) -> str | None:
    """
    Visit a DullesMoms event detail page and extract the best outbound URL.

    DullesMoms list-page articles rarely carry outbound links, but the
    per-event detail page usually has a "Website", "Register", or "Tickets"
    button pointing to the original host.  This function must NOT extract or
    store any descriptive content from the DullesMoms page.
    """
    _SEED_DOMAIN = "dullesmoms.com"

    # Domains that are NOT usable original event sources
    _UTILITY_DOMAINS = frozenset([
        "google.com", "calendar.google.com",
        "apple.com", "outlook.live.com", "outlook.office.com",
        "facebook.com", "instagram.com", "twitter.com", "x.com",
        "linkedin.com", "youtube.com",
        "maps.google.com", "goo.gl",
    ])

    def _is_dm_url(url: str) -> bool:
        try:
            return _SEED_DOMAIN in urlparse(url).netloc.lower()
        except Exception:
            return True

    def _is_utility_url(url: str) -> bool:
        """Return True if the URL is a utility/social link, not an original event host."""
        try:
            netloc = urlparse(url).netloc.lower().lstrip("www.")
            return any(netloc == d or netloc.endswith("." + d) for d in _UTILITY_DOMAINS)
        except Exception:
            return False

    try:
        time.sleep(REQUEST_DELAY)
        resp = session.get(seed_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        logger.debug("Could not fetch DullesMoms detail page %s: %s", seed_url, exc)
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    priority_re = re.compile(
        r"original|register|ticket|sign.?up|event details|learn more|website|source|buy",
        re.IGNORECASE,
    )

    # Priority 1: links whose text or href suggest they are the original source,
    #             that are neither DullesMoms nor utility/social/calendar links
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        if not href.startswith("http"):
            continue
        if _is_dm_url(href) or _is_utility_url(href):
            continue
        link_text = anchor.get_text(strip=True)
        if priority_re.search(link_text) or priority_re.search(href):
            return href

    # Priority 2: first outbound https link that is not a utility/social URL
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        if (
            href.startswith("https://")
            and not _is_dm_url(href)
            and not _is_utility_url(href)
        ):
            return href

    return None


_GENERIC_TITLE_RE = re.compile(
    r"^(home\s*page?|welcome|untitled|index|coming\s*soon|error|404|403|not\s*found"
    r"|page\s*not\s*found|access\s*denied|forbidden)$",
    re.IGNORECASE,
)


def _is_generic_title(title: str | None) -> bool:
    """Return True if the title looks like a generic page title, not an event title."""
    if not title:
        return True
    t = title.strip()
    # Very short non-descriptive titles
    if len(t) <= 6:
        return True
    return bool(_GENERIC_TITLE_RE.match(t))


def _source_name_from_url(url: str) -> str:
    """Derive a readable source name from a URL domain."""
    try:
        domain = urlparse(url).netloc.lower()
        domain = re.sub(r"^www\.", "", domain)
        domain = re.sub(r"\.\w+$", "", domain)  # drop TLD
        return domain.replace("-", " ").replace("_", " ").title()
    except Exception:
        return "Unknown Source"


def _compute_extraction_confidence(
    facts: dict[str, Any],
    discovered: dict[str, Any] | None = None,
) -> float:
    """
    Score how complete the extracted facts are (0–1).

    When `discovered` is provided, fields missing from `facts` can be
    supplemented with discovered_ candidate data at a reduced credit weight.
    This lets events with a valid original URL but sparse structured data
    still be published when we have reliable seed discovery metadata.

    Used to decide auto-publish vs. manual review.
    """
    disc = discovered or {}
    score = 0.0

    if facts.get("title"):
        score += 0.30
    elif disc.get("title"):
        score += 0.15  # partial credit for discovered fallback

    if facts.get("start_text"):
        score += 0.30
    elif disc.get("start_text"):
        score += 0.20  # date from DullesMoms is fairly reliable

    if facts.get("location_name") or facts.get("location_address"):
        score += 0.20
    elif disc.get("location_name"):
        score += 0.10  # partial credit for discovered location

    if facts.get("cost_text") is not None:
        score += 0.10

    if facts.get("description_snippet"):
        score += 0.05

    if facts.get("registration_url"):
        score += 0.05

    return round(score, 4)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def resolve_candidate(
    candidate: CandidateEvent,
    session: requests.Session | None = None,
) -> dict[str, Any] | None:
    """
    Fetch the original event page and extract structured facts.

    Returns:
      - A raw dict (suitable for normalize_record()) when extraction succeeds
        with sufficient confidence.
      - None when the candidate should be routed to manual review; in that
        case, `candidate` is mutated to reflect the failure reason.

    The returned dict includes:
      - All standard normalize_record() fields
      - extracted_from = "seed_resolved"
      - extraction_confidence = float
      - source_name = original host name
      - source_url = original host URL
    """
    original_url = candidate.candidate_original_url
    _session = session or _build_session()

    # If no original URL was found on the list page, visit the DullesMoms
    # event detail page to look for an outbound link there.
    if not original_url and candidate.seed_url and "dullesmoms.com" in candidate.seed_url:
        logger.debug(
            "No original URL for '%s' — visiting DullesMoms detail page: %s",
            candidate.discovered_title,
            candidate.seed_url,
        )
        original_url = _find_original_url_from_detail_page(candidate.seed_url, _session)
        if original_url:
            candidate.candidate_original_url = original_url
            # Recalculate confidence now that we have an original URL
            candidate.confidence = min(candidate.confidence + 0.50, 1.0)
            candidate.requires_manual_review = False
            logger.info(
                "Detail-page resolution: found original URL for '%s': %s",
                candidate.discovered_title,
                original_url,
            )
        else:
            candidate.requires_manual_review = True
            candidate.status = CandidateStatus.MANUAL_REVIEW
            candidate.review_reason = "no_original_url_found"
            candidate.last_resolution_error = (
                "No outbound original URL found on DullesMoms list page or detail page."
            )
            candidate.suggested_next_action = (
                "Search for this event manually and set candidate_original_url."
            )
            candidate.notes = candidate.last_resolution_error
            return None

    if not original_url:
        candidate.requires_manual_review = True
        candidate.status = CandidateStatus.MANUAL_REVIEW
        candidate.review_reason = "no_original_url_found"
        candidate.notes = (candidate.notes or "") + " No original URL to resolve."
        return None

    # Fetch the original page
    try:
        time.sleep(REQUEST_DELAY)
        response = _session.get(original_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Could not fetch original URL %s: %s", original_url, exc)
        candidate.requires_manual_review = True
        candidate.status = CandidateStatus.MANUAL_REVIEW
        candidate.review_reason = "original_url_dead"
        candidate.last_resolution_error = f"Fetch failed: {exc}"
        candidate.suggested_next_action = (
            "Verify the URL is still live and update candidate_original_url if needed."
        )
        candidate.notes = (
            (candidate.notes or "") + f" Fetch failed: {exc}"
        ).strip()
        return None

    soup = BeautifulSoup(response.text, "lxml")

    # Extract facts in priority order from the original page
    jsonld_facts = _extract_jsonld(soup)
    og_facts = _extract_opengraph(soup)
    html_facts = _extract_html_patterns(soup)
    facts = _merge_facts(jsonld_facts, og_facts, html_facts)

    # Discovered fields available as fallbacks (from DullesMoms, marked non-authoritative)
    discovered_fallbacks: dict[str, Any] = {}
    if candidate.discovered_title:
        discovered_fallbacks["title"] = candidate.discovered_title
    if candidate.discovered_date_text:
        discovered_fallbacks["start_text"] = candidate.discovered_date_text
    if candidate.discovered_location_text:
        discovered_fallbacks["location_name"] = candidate.discovered_location_text

    # Compute confidence with discovered fallbacks providing partial credit
    extraction_confidence = _compute_extraction_confidence(facts, discovered=discovered_fallbacks)

    # Populate candidate extracted fields (original page first, fallback to discovered)
    # Prefer the discovered title (which DullesMoms specifically wrote for this event)
    # over a generic/venue-homepage title extracted from the original page.
    raw_extracted_title = facts.get("title")
    if _is_generic_title(raw_extracted_title):
        raw_extracted_title = None
    # Prefer discovered title when:
    # (a) extracted is a substring of discovered (venue name embedded in event name)
    # (b) extracted looks like a website page title ("Venue | Location", "Venue - Site")
    if raw_extracted_title and candidate.discovered_title:
        is_venue_substring = (
            raw_extracted_title.lower() in candidate.discovered_title.lower()
            and len(candidate.discovered_title) > len(raw_extracted_title) + 5
        )
        is_page_title = " | " in raw_extracted_title or (
            " - " in raw_extracted_title
            and not any(kw in raw_extracted_title.lower() for kw in
                        ["storytime", "workshop", "class", "camp", "tour", "event"])
        )
        if is_venue_substring or is_page_title:
            raw_extracted_title = None
    candidate.extracted_title = raw_extracted_title or candidate.discovered_title
    candidate.extracted_date_text = facts.get("start_text") or candidate.discovered_date_text
    candidate.extracted_venue = facts.get("location_name") or candidate.discovered_location_text
    candidate.extracted_address = facts.get("location_address")
    candidate.extracted_cost_text = facts.get("cost_text")
    candidate.extracted_description_snippet = facts.get("description_snippet")
    candidate.extracted_registration_url = facts.get("registration_url")
    candidate.original_source_name = (
        facts.get("source_name_hint")
        or _source_name_from_url(original_url)
    )
    candidate.resolved_at = datetime.now(tz=timezone.utc)

    # Overall confidence: weighted average of discovery and extraction signals
    combined_confidence = round(
        (candidate.confidence * 0.3) + (extraction_confidence * 0.7),
        4,
    )
    candidate.confidence = combined_confidence

    if combined_confidence < _MIN_PUBLISH_CONFIDENCE:
        candidate.requires_manual_review = True
        candidate.status = CandidateStatus.MANUAL_REVIEW
        candidate.review_reason = "low_confidence"
        candidate.last_resolution_error = (
            f"Extraction confidence {combined_confidence:.2f} below threshold "
            f"{_MIN_PUBLISH_CONFIDENCE}."
        )
        candidate.suggested_next_action = (
            "Check the original URL for structured data (JSON-LD) or review "
            "manually to confirm title, date, and location."
        )
        candidate.notes = (
            (candidate.notes or "")
            + f" Extraction confidence {combined_confidence:.2f} below threshold."
        ).strip()
        logger.info(
            "Candidate '%s' → manual review (confidence %.2f)",
            candidate.discovered_title,
            combined_confidence,
        )
        return None

    candidate.status = CandidateStatus.RESOLVED

    # Build the raw dict for normalize_record()
    # Use extracted facts first; fall back to discovered_ fields for title/date/location
    location_text = " ".join(
        filter(None, [candidate.extracted_venue, candidate.extracted_address])
    ) or candidate.discovered_location_text

    raw: dict[str, Any] = {
        "title": candidate.extracted_title,
        "start_text": candidate.extracted_date_text,
        "end_text": None,
        "location_text": location_text,
        "price_text": candidate.extracted_cost_text,
        "summary_text": candidate.extracted_description_snippet,
        "source_name": candidate.original_source_name,
        "source_url": original_url,
        "registration_url": candidate.extracted_registration_url,
        "image_url": None,  # never copy images from seed sources
        # Provenance fields consumed by normalize_record()
        "extracted_from": "seed_resolved",
        "extraction_confidence": combined_confidence,
    }

    logger.info(
        "Candidate '%s' resolved (confidence %.2f) → source: %s",
        candidate.discovered_title,
        combined_confidence,
        original_url,
    )
    return raw


def resolve_candidates(
    candidates: list[CandidateEvent],
) -> tuple[list[dict[str, Any]], list[CandidateEvent]]:
    """
    Resolve a batch of candidates, including those previously flagged for review.

    For candidates without a candidate_original_url, this will attempt to visit
    the DullesMoms event detail page to find one before giving up.

    Returns:
      (resolved_raws, manual_review_candidates)

    resolved_raws: list of raw dicts ready for normalize_record()
    manual_review_candidates: list of CandidateEvents that still need human review
    """
    session = _build_session()
    resolved_raws: list[dict[str, Any]] = []
    manual_review: list[CandidateEvent] = []

    for candidate in candidates:
        # Skip explicitly rejected or already published candidates
        if candidate.status.value in ("rejected", "published"):
            continue

        candidate.resolution_attempts += 1
        raw = resolve_candidate(candidate, session=session)
        if raw is not None:
            resolved_raws.append(raw)
        else:
            manual_review.append(candidate)

    logger.info(
        "Resolution complete: %d resolved, %d → manual review",
        len(resolved_raws),
        len(manual_review),
    )
    return resolved_raws, manual_review
