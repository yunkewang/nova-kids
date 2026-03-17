"""
DullesMoms seed finder — discovery only.

PURPOSE
-------
This module visits the DullesMoms public calendar list page SOLELY to
identify candidate events and, where present, outbound links pointing to the
original event host pages.

WHAT IT STORES
--------------
  - discovered_title: the event title as displayed (for matching purposes)
  - discovered_date_text: raw date string (for matching purposes)
  - discovered_location_text: raw location string (for matching purposes)
  - candidate_original_url: the first non-DullesMoms outbound link, if found

WHAT IT DOES NOT STORE
-----------------------
  - Descriptions, summaries, or body text from DullesMoms pages
  - Images or media from DullesMoms
  - DullesMoms internal page URLs as source_url (those are seed URLs only)

OUTPUT
------
Returns list[CandidateEvent].  Each candidate must pass through the resolver
(seed_discovery/resolver.py) before any data is normalized or published.
Candidates without an original URL are routed to manual review automatically.

LEGAL / PRODUCT NOTE
---------------------
DullesMoms is a community aggregator.  Published app content must come from
original event host pages, not from DullesMoms.  This module exists only to
speed up discovery of events we might otherwise miss.  If in doubt, route
to manual review.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

from models.candidate import CandidateEvent, CandidateStatus
from seed_discovery.base import BaseSeedFinder

logger = logging.getLogger(__name__)

SEED_URL = "https://dullesmoms.com/dmcalendar/list/"
SEED_DOMAIN = "dullesmoms.com"


def _is_dullesmoms_url(url: str) -> bool:
    """Return True if the URL belongs to dullesmoms.com."""
    try:
        return SEED_DOMAIN in urlparse(url).netloc.lower()
    except Exception:
        return True  # treat unparseable URLs as seed-domain URLs


def _stable_candidate_id(seed_url: str, title: str) -> str:
    """Generate a stable candidate ID from seed URL + title."""
    raw = f"{seed_url}|{title.lower().strip()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class DullesMomsSeedFinder(BaseSeedFinder):
    """
    Discovers candidate events from the DullesMoms calendar list page.

    Selectors target The Events Calendar plugin (tribe-events), which
    DullesMoms uses.  They should be verified on the live page if events
    stop appearing.
    """

    seed_source_name = "DullesMoms"

    def __init__(self, target_week_start: date | None = None) -> None:
        super().__init__()
        self.target_week_start = target_week_start

    def _candidate_in_target_week(self, date_text: str | None) -> bool:
        """Return True if date_text falls within [target_week_start, +6 days]."""
        if self.target_week_start is None or not date_text:
            return True
        try:
            dt = dateutil_parser.parse(date_text, fuzzy=True)
            week_end = self.target_week_start + timedelta(days=6)
            return self.target_week_start <= dt.date() <= week_end
        except Exception:
            return True  # unparseable date → include rather than drop

    def fetch_candidates(self) -> list[CandidateEvent]:
        candidates: list[CandidateEvent] = []
        url: str | None = SEED_URL

        while url:
            logger.debug("Fetching seed page: %s", url)
            try:
                response = self.get(url)
            except Exception as exc:
                logger.warning("Failed to fetch seed page %s: %s", url, exc)
                break

            soup = BeautifulSoup(response.text, "lxml")
            event_articles = soup.select(
                # The Events Calendar plugin: list view articles
                "article.type-tribe_events, "
                ".tribe-events-calendar-list__event-article, "
                # Fallback: generic event list items
                "div.tribe-event, li.tribe-event"
            )

            if not event_articles:
                logger.debug("No event articles found on %s — stopping.", url)
                break

            for article in event_articles:
                candidate = self._parse_article(article)
                if candidate:
                    if self._candidate_in_target_week(candidate.discovered_date_text):
                        candidates.append(candidate)
                    else:
                        logger.debug(
                            "Skipping '%s' (%s) — outside target week.",
                            candidate.discovered_title,
                            candidate.discovered_date_text,
                        )

            # Pagination: follow "next" link
            next_el = soup.select_one("a.tribe-events-c-nav__next, a[rel='next']")
            url = next_el["href"] if next_el else None

        logger.info("DullesMoms seed: discovered %d candidates.", len(candidates))
        return candidates

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_article(self, article: BeautifulSoup) -> CandidateEvent | None:
        """
        Extract only the minimum necessary fields from a single event article.

        We deliberately avoid touching the event description or body text.
        """
        # Title
        title_el = article.select_one(
            ".tribe-events-calendar-list__event-title a, "
            "h2.tribe-events-list-event-title a, "
            "h3.tribe-event-url a, "
            "h2 a, h3 a"
        )
        if not title_el:
            return None

        discovered_title = title_el.get_text(strip=True)
        if not discovered_title:
            return None

        # The DullesMoms page URL for this event (seed URL only — not published)
        dullesmoms_href = title_el.get("href", "") or ""
        seed_event_url = (
            dullesmoms_href
            if dullesmoms_href.startswith("http")
            else urljoin(SEED_URL, dullesmoms_href)
        )

        # Date text — used for matching only, not stored as content
        date_el = article.select_one(
            "abbr.tribe-events-abbr, "
            "time.tribe-event-date-start, "
            ".tribe-events-schedule abbr, "
            ".tribe-event-date-start"
        )
        discovered_date_text: str | None = None
        if date_el:
            discovered_date_text = (
                date_el.get("title") or date_el.get_text(strip=True) or None
            )

        # Location text — used for matching only
        venue_el = article.select_one(
            ".tribe-venue, "
            ".tribe-events-calendar-list__event-venue-title, "
            ".tribe-venue-location"
        )
        discovered_location_text: str | None = (
            venue_el.get_text(strip=True) if venue_el else None
        )

        # Outbound / original URL — the key goal of this scraper
        candidate_original_url = self._find_original_url(article, seed_event_url)

        # Confidence: higher when we have an original URL and a date
        confidence = self._compute_confidence(
            candidate_original_url=candidate_original_url,
            has_date=bool(discovered_date_text),
            has_location=bool(discovered_location_text),
        )

        notes: str | None = None
        if candidate_original_url is None:
            notes = "No outbound original URL found on seed page. Manual review required."

        return CandidateEvent(
            candidate_id=_stable_candidate_id(seed_event_url, discovered_title),
            seed_source_name=self.seed_source_name,
            seed_url=seed_event_url,
            discovered_title=discovered_title,
            discovered_date_text=discovered_date_text,
            discovered_location_text=discovered_location_text,
            candidate_original_url=candidate_original_url,
            confidence=confidence,
            requires_manual_review=(candidate_original_url is None or confidence < 0.5),
            notes=notes,
            discovered_at=datetime.now(tz=timezone.utc),
        )

    def _find_original_url(
        self,
        article: BeautifulSoup,
        seed_event_url: str,
    ) -> str | None:
        """
        Search an event article for an outbound (non-DullesMoms) URL.

        Strategy:
          1. Look for explicit "original event" or "register here" links.
          2. Scan all anchors for the first non-DullesMoms https URL.

        Returns the URL string, or None if nothing suitable is found.
        """
        # Priority 1: links with text suggesting official source
        priority_patterns = re.compile(
            r"original|register|ticket|sign.?up|event details|learn more|website|source",
            re.IGNORECASE,
        )
        for anchor in article.find_all("a", href=True):
            href = anchor.get("href", "")
            if not href.startswith("http"):
                continue
            if _is_dullesmoms_url(href):
                continue
            link_text = anchor.get_text(strip=True)
            if priority_patterns.search(link_text) or priority_patterns.search(href):
                return href

        # Priority 2: first non-DullesMoms outbound https link
        for anchor in article.find_all("a", href=True):
            href = anchor.get("href", "")
            if href.startswith("https://") and not _is_dullesmoms_url(href):
                return href

        return None

    @staticmethod
    def _compute_confidence(
        candidate_original_url: str | None,
        has_date: bool,
        has_location: bool,
    ) -> float:
        """
        Compute a 0–1 confidence score for this candidate.

        Scoring:
          +0.50 has a non-DullesMoms original URL
          +0.25 has a date string (parseable later)
          +0.15 has a location string
          +0.10 original URL uses https
        """
        score = 0.0
        if candidate_original_url:
            score += 0.50
            if candidate_original_url.startswith("https://"):
                score += 0.10
        if has_date:
            score += 0.25
        if has_location:
            score += 0.15
        return round(min(score, 1.0), 4)
