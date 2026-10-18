"""
Tests for the NOVA Parks AJAX-endpoint scraper (scrapers/nova_parks.py).

Uses an offline fixture (tests/fixtures/nova_parks_rows.html) built from real
server-rendered row markup (captured 2026-10-18) with four representative
rows:

  1. date-range row      "Oct 16 - 18",        time "6:00pm - 10:00am"
  2. single-day row      "Oct 18",             time "10:00am - 2:00pm"
  3. month-crossing row  "Oct 30 - Nov 1",     time "4:00pm - 9:00pm"
  4. no image / no time  "Oct 18",             time missing
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from scrapers.nova_parks import (
    NoVAParksScraper,
    _is_adult_only,
    _parse_date_text,
    _resolve_date,
    _split_time,
)

FIXTURE = Path(__file__).parent / "fixtures" / "nova_parks_rows.html"

# Window the fixture rows were captured in: 2026-10-18 -> 2026-11-18.
WINDOW_FROM = date(2026, 10, 18)


@pytest.fixture(scope="module")
def rows():
    soup = BeautifulSoup(FIXTURE.read_text(encoding="utf-8"), "lxml")
    return soup.select("div.views-row")


@pytest.fixture(scope="module")
def scraper():
    return NoVAParksScraper()


class TestParseDateText:
    def test_single_day(self):
        start, end = _parse_date_text("Oct 18", WINDOW_FROM)
        assert start == date(2026, 10, 18)
        assert end == date(2026, 10, 18)

    def test_same_month_range(self):
        start, end = _parse_date_text("Oct 16 - 18", WINDOW_FROM)
        assert start == date(2026, 10, 16)
        assert end == date(2026, 10, 18)

    def test_month_crossing_range(self):
        start, end = _parse_date_text("Oct 30 - Nov 1", WINDOW_FROM)
        assert start == date(2026, 10, 30)
        assert end == date(2026, 11, 1)

    def test_dec_to_jan_rollover(self):
        # Window starting in December: a "Jan 5" row belongs to next year.
        start, end = _parse_date_text("Jan 5", date(2026, 12, 10))
        assert start == date(2027, 1, 5)
        assert end == date(2027, 1, 5)

    def test_dec_to_jan_range_rollover(self):
        start, end = _parse_date_text("Dec 30 - Jan 1", date(2026, 12, 10))
        assert start == date(2026, 12, 30)
        assert end == date(2027, 1, 1)

    def test_garbage_returns_none(self):
        assert _parse_date_text("coming soon", WINDOW_FROM) == (None, None)
        assert _parse_date_text("", WINDOW_FROM) == (None, None)


class TestResolveDate:
    def test_same_year(self):
        assert _resolve_date(10, 18, WINDOW_FROM) == date(2026, 10, 18)

    def test_rolls_forward_when_before_window(self):
        # "Jan 5" with an Oct-2026 window start resolves to 2027.
        assert _resolve_date(1, 5, WINDOW_FROM) == date(2027, 1, 5)

    def test_feb_29_falls_forward(self):
        # Non-leap year: Feb 29 cannot exist in 2027 -> next valid year.
        assert _resolve_date(2, 29, date(2027, 1, 1)) == date(2028, 2, 29)


class TestSplitTime:
    def test_range(self):
        assert _split_time("6:00pm - 10:00am") == ("6:00pm", "10:00am")

    def test_single(self):
        assert _split_time("10:00am") == ("10:00am", None)

    def test_missing(self):
        assert _split_time(None) == (None, None)
        assert _split_time("") == (None, None)


class TestIsAdultOnly:
    def test_beer_event_excluded(self):
        assert _is_adult_only("Barks, Brews, and BOOs!", "Craft beer tasting")

    def test_family_event_kept(self):
        assert not _is_adult_only("Paint a Pumpkin", "Family pumpkin painting")

    def test_shooting_excluded(self):
        assert _is_adult_only("Three Bird Fun Shoot (Non-NSCA)", None)


class TestParseRow:
    def test_four_rows_parsed(self, scraper, rows):
        assert len(rows) == 4
        parsed = [scraper._parse_row(r, WINDOW_FROM) for r in rows]
        assert all(p is not None for p in parsed)

    def test_range_row(self, scraper, rows):
        rec = scraper._parse_row(rows[0], WINDOW_FROM)
        assert rec["title"] == "Weekend Campground Activities"
        assert rec["source_url"] == (
            "https://www.novaparks.com/parks/pohick-bay-regional-park/"
            "events/weekend-campground-activities-23"
        )
        assert rec["date_text"] == "Oct 16 - 18"
        assert rec["start_text"] == "2026-10-16, 6:00pm"
        assert rec["end_text"] == "2026-10-18, 10:00am"
        assert rec["location_text"] == "Pohick Bay Regional Park, Lorton, VA"
        assert rec["summary_text"].startswith("Make the weekend memorable")
        assert rec["image_url"].startswith(
            "https://www.novaparks.com/sites/default/files/"
        )
        # Placeholder gif src must never leak into image_url
        assert "data:image" not in rec["image_url"]
        assert rec["source_id"] == "nova_parks"

    def test_single_day_row(self, scraper, rows):
        rec = scraper._parse_row(rows[1], WINDOW_FROM)
        assert rec["title"] == "Roving Naturalist: Critter Corner"
        assert rec["start_text"] == "2026-10-18, 10:00am"
        assert rec["end_text"] == "2026-10-18, 2:00pm"

    def test_month_crossing_row(self, scraper, rows):
        rec = scraper._parse_row(rows[2], WINDOW_FROM)
        assert rec["title"] == "Halloween at Upton Hill"
        assert rec["start_text"] == "2026-10-30, 4:00pm"
        assert rec["end_text"] == "2026-11-01, 9:00pm"

    def test_missing_image_and_time(self, scraper, rows):
        rec = scraper._parse_row(rows[3], WINDOW_FROM)
        assert rec["title"] == "Roving Naturalist: Critter Corner"
        assert rec["image_url"] is None
        assert rec["start_text"] == "2026-10-18"
        assert rec["end_text"] == "2026-10-18"

    def test_adult_only_row_skipped(self, scraper, rows):
        row = BeautifulSoup(str(rows[1]), "lxml").select_one("div.views-row")
        row.select_one("h3 a").string = "Barks, Brews, and BOOs!"
        assert scraper._parse_row(row, WINDOW_FROM) is None

    def test_window_url_shape(self, scraper):
        url = scraper._window_url(date(2026, 10, 18), date(2026, 11, 16))
        assert url.startswith("https://www.novaparks.com/ajax-event-calendar?")
        assert "from=2026-10-18%2000:00:01" in url
        assert "to=2026-11-16%2023:59:59" in url
        assert "category=All" in url
        assert "response_type=ajax" in url
