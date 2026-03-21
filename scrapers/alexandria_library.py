"""
Scraper for Alexandria Library (City of Alexandria, VA) events.

Source: https://alexandria.libnet.info/kids
Type:   HTML (BeautifulSoup) — Communico/LibNet platform

Alexandria Library uses LibNet (Communico) at alexandria.libnet.info.
The /kids page server-side-renders family and children's events for
approximately 30 days in standard `.amev-event` blocks.

Event structure in HTML:
  div.amev-event
    div.amev-event-title > a (title + source URL)
    div.amev-event-time   (date/time text)
    div.amev-event-location (venue + room)
    div.amev-event-description (may have description)

Branch locations served:
  - Charles E. Beatley Jr. Central Library (Alexandria, VA)
  - Kate Waller Barrett Branch Library (Alexandria, VA)
  - Ellen Coolidge Burke Branch Library (Alexandria, VA)
  - James M. Duncan Jr. Branch Library (Alexandria, VA)
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL   = "https://alexandria.libnet.info"
# The /kids page pre-selects family and children's events — no keyword filter needed
EVENTS_URL = f"{BASE_URL}/kids"


class AlexandriaLibraryScraper(BaseScraper):
    """Scrapes children/family events from Alexandria Library via LibNet HTML (/kids page)."""

    source_id   = "alexandria_library"
    source_name = "Alexandria Library"

    def fetch_raw(self) -> list[dict[str, Any]]:
        try:
            resp = self.get(EVENTS_URL)
        except Exception as exc:
            logger.warning("Alexandria Library: failed to fetch events page: %s", exc)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        records = self._parse_events(soup)
        logger.debug("Fetched %d events from %s", len(records), self.source_name)
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
            records.append(raw)

        return records

    def _parse_card(self, card: BeautifulSoup) -> dict[str, Any] | None:
        title_el = card.select_one("div.amev-event-title a")
        if not title_el:
            return None

        # Title: strip subtitle before reading main title
        subtitle_el = title_el.select_one("span.amev-event-subtitle")
        if subtitle_el:
            subtitle_el.extract()
        title = title_el.get_text(strip=True)
        if not title:
            return None

        href = title_el.get("href", "")
        if not href:
            return None
        event_url = href if href.startswith("http") else urljoin(BASE_URL, href)

        # Date/time: "Mon, Mar 23, 11:00am - 11:30am" or "Mon, Mar 23, All day"
        time_el = card.select_one("div.amev-event-time")
        start_text: str | None = None
        end_text: str | None = None
        all_day: bool = False
        if time_el:
            raw_dt = time_el.get_text(strip=True)
            if raw_dt:
                parts = raw_dt.split(", ", 2)  # ["Mon", "Mar 23", "11:00am - 11:30am"]
                if len(parts) == 3:
                    date_part = ", ".join(parts[:2])  # "Mon, Mar 23"
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

        # Location: "Charles E. Beatley Jr. Central Library - Frank and Betty Wright Reading Garden"
        loc_el = card.select_one("div.amev-event-location")
        location_text: str | None = None
        if loc_el:
            for icon in loc_el.select("i.am-locations"):
                icon.extract()
            raw_loc = loc_el.get_text(separator=" ", strip=True)
            # Keep only the branch name (before " - Room Name")
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
