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


def _source_name_from_url(url: str) -> str:
    """Derive a readable source name from a URL domain."""
    try:
        domain = urlparse(url).netloc.lower()
        domain = re.sub(r"^www\.", "", domain)
        domain = re.sub(r"\.\w+$", "", domain)  # drop TLD
        return domain.replace("-", " ").replace("_", " ").title()
    except Exception:
        return "Unknown Source"


def _compute_extraction_confidence(facts: dict[str, Any]) -> float:
    """
    Score how complete the extracted facts are (0–1).

    Used to decide auto-publish vs. manual review.
    """
    score = 0.0
    if facts.get("title"):
        score += 0.30
    if facts.get("start_text"):
        score += 0.30
    if facts.get("location_name") or facts.get("location_address"):
        score += 0.20
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
    if not original_url:
        candidate.requires_manual_review = True
        candidate.status = CandidateStatus.MANUAL_REVIEW
        candidate.notes = (candidate.notes or "") + " No original URL to resolve."
        return None

    _session = session or _build_session()

    # Fetch the original page
    try:
        time.sleep(REQUEST_DELAY)
        response = _session.get(original_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Could not fetch original URL %s: %s", original_url, exc)
        candidate.requires_manual_review = True
        candidate.status = CandidateStatus.MANUAL_REVIEW
        candidate.notes = (
            (candidate.notes or "") + f" Fetch failed: {exc}"
        ).strip()
        return None

    soup = BeautifulSoup(response.text, "lxml")

    # Extract facts in priority order
    jsonld_facts = _extract_jsonld(soup)
    og_facts = _extract_opengraph(soup)
    html_facts = _extract_html_patterns(soup)
    facts = _merge_facts(jsonld_facts, og_facts, html_facts)

    extraction_confidence = _compute_extraction_confidence(facts)

    # Update candidate with extracted data
    candidate.extracted_title = facts.get("title") or candidate.discovered_title
    candidate.extracted_date_text = facts.get("start_text") or candidate.discovered_date_text
    candidate.extracted_venue = facts.get("location_name")
    candidate.extracted_address = facts.get("location_address")
    candidate.extracted_cost_text = facts.get("cost_text")
    candidate.extracted_description_snippet = facts.get("description_snippet")
    candidate.extracted_registration_url = facts.get("registration_url")
    candidate.original_source_name = (
        facts.get("source_name_hint")
        or _source_name_from_url(original_url)
    )
    candidate.resolved_at = datetime.now(tz=timezone.utc)

    # Update overall candidate confidence
    combined_confidence = round(
        (candidate.confidence * 0.3) + (extraction_confidence * 0.7),
        4,
    )
    candidate.confidence = combined_confidence

    if combined_confidence < _MIN_PUBLISH_CONFIDENCE:
        candidate.requires_manual_review = True
        candidate.status = CandidateStatus.MANUAL_REVIEW
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
    raw: dict[str, Any] = {
        "title": candidate.extracted_title or candidate.discovered_title,
        "start_text": candidate.extracted_date_text,
        "end_text": None,
        "location_text": " ".join(
            filter(None, [candidate.extracted_venue, candidate.extracted_address])
        ) or candidate.discovered_location_text,
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
    Resolve a batch of candidates.

    Returns:
      (resolved_raws, manual_review_candidates)

    resolved_raws: list of raw dicts ready for normalize_record()
    manual_review_candidates: list of CandidateEvents that need human review
    """
    session = _build_session()
    resolved_raws: list[dict[str, Any]] = []
    manual_review: list[CandidateEvent] = []

    for candidate in candidates:
        if candidate.requires_manual_review:
            manual_review.append(candidate)
            continue

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
