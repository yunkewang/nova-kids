"""
Scraper for Loudoun County Public Library (LCPL) events.

Source: https://loudoun.libnet.info/event-calendar
Type:   HTML (BeautifulSoup) — Communico/LibNet platform

Loudoun County Public Library uses LibNet (Communico) at loudoun.libnet.info.
Their event-calendar page server-side-renders events for approximately 30 days
in standard `.amev-event` blocks that can be scraped without JavaScript.

Event structure in HTML:
  div.amev-event
    div.amev-event-title > a (title + source URL)
    div.amev-event-time   (date/time text)
    div.amev-event-location (venue + room)
    div.amev-event-description (may be empty)

Family relevance filter applied client-side; adult-only events excluded.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL   = "https://loudoun.libnet.info"
EVENTS_URL = f"{BASE_URL}/event-calendar"

# Keywords that identify family/children-relevant events (case-insensitive)
_FAMILY_KEYWORDS = re.compile(
    r"\b(?:child|children|kid|kids|family|families|toddler|preschool|baby|babies|"
    r"youth|storytime|story\s*time|elementary|juvenile|teen|tween|infant|"
    r"lap\s*sit|lego|little\s+ones|wee\s+ones|junior|play\s*time|"
    r"early\s+literacy|reading\s+aloud|young\s+readers?|"
    r"homework\s+help|craft\s+time|science\s+club|math\s+club)\b",
    re.IGNORECASE,
)

# Adult-only exclusions — skip events that clearly don't apply to families
_ADULT_ONLY_RE = re.compile(
    r"\b(?:adult\s+only|21\+|wine\s+tasting|beer\s+tasting|cocktail|"
    r"senior\s+only|alzheimer|dementia|caregiver\s+support|grief\s+group|"
    r"job\s+seeker|resume\s+workshop)\b",
    re.IGNORECASE,
)


def _is_family_event(title: str, description: str = "") -> bool:
    combined = f"{title} {description}"
    if _ADULT_ONLY_RE.search(combined):
        return False
    return bool(_FAMILY_KEYWORDS.search(combined))


class LoudounLibraryScraper(BaseScraper):
    """Scrapes children/family events from Loudoun County Public Library via LibNet HTML."""

    source_id   = "loudoun_county_library"
    source_name = "Loudoun County Public Library"

    def fetch_raw(self) -> list[dict[str, Any]]:
        try:
            resp = self.get(EVENTS_URL)
        except Exception as exc:
            logger.warning("LCPL: failed to fetch events page: %s", exc)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        records = self._parse_events(soup)
        logger.debug("Fetched %d family events from %s", len(records), self.source_name)
        return records

    def _parse_events(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for card in soup.select("div.amev-event"):
            raw = self._parse_card(card)
            if raw is None:
                continue
            url = raw.get("source_url", "")
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            if _is_family_event(raw.get("title", ""), raw.get("summary_text") or ""):
                records.append(raw)

        return records

    def _parse_card(self, card: BeautifulSoup) -> dict[str, Any] | None:
        title_el = card.select_one("div.amev-event-title a")
        if not title_el:
            return None

        # Title: combine main title and subtitle
        subtitle_el = title_el.select_one("span.amev-event-subtitle")
        subtitle = subtitle_el.get_text(strip=True) if subtitle_el else ""
        if subtitle_el:
            subtitle_el.extract()
        title = title_el.get_text(strip=True)
        if not title:
            return None

        href = title_el.get("href", "")
        if not href:
            return None
        event_url = href if href.startswith("http") else urljoin(BASE_URL, href)

        # Date/time: "Fri, Mar 20, 10:00am - 11:00am" or "Fri, Mar 20, All day"
        time_el = card.select_one("div.amev-event-time")
        start_text: str | None = None
        end_text: str | None = None
        all_day: bool = False
        if time_el:
            raw_dt = time_el.get_text(strip=True)
            if raw_dt:
                # Format: "Day, Mon DD, HH:MMam - HH:MMam" or "Day, Mon DD, All day"
                # Split off the time portion after the second comma
                parts = raw_dt.split(", ", 2)  # ["Fri", "Mar 20", "10:00am - 11:00am"]
                if len(parts) == 3:
                    date_part = ", ".join(parts[:2])  # "Fri, Mar 20"
                    time_part = parts[2]
                    if time_part.lower() in ("all day", "all-day"):
                        start_text = date_part
                        all_day = True
                    elif " - " in time_part:
                        t_start, t_end = time_part.split(" - ", 1)
                        start_text = f"{date_part}, {t_start.strip()}"
                        end_text   = f"{date_part}, {t_end.strip()}"
                    else:
                        start_text = f"{date_part}, {time_part}"
                else:
                    start_text = raw_dt

        # Location: "Ashburn Library - Meeting Room B"
        loc_el = card.select_one("div.amev-event-location")
        location_text: str | None = None
        if loc_el:
            # Remove the icon element before reading text
            for icon in loc_el.select("i.am-locations"):
                icon.extract()
            raw_loc = loc_el.get_text(separator=" ", strip=True)
            # Split room suffix ("Library - Room Name") — keep the library part
            if " - " in raw_loc:
                location_text = raw_loc.split(" - ")[0].strip() or None
            else:
                location_text = raw_loc or None

        # Description
        desc_el = card.select_one("div.amev-event-description")
        summary_text: str | None = None
        if desc_el:
            summary_text = desc_el.get_text(separator=" ", strip=True) or None

        return {
            "source_id":     self.source_id,
            "source_name":   self.source_name,
            "source_url":    event_url,
            "title":         title,
            "start_text":    start_text,
            "end_text":      end_text,
            "all_day":       all_day,
            "location_text": location_text,
            "summary_text":  summary_text,
        }
