"""
Tests for the curated marquee-events source.

The curated source is the one place in the pipeline where event facts are typed
in by a human, so these tests concentrate on the guardrails that keep a stale or
malformed file from publishing wrong data: past-date pruning, the forward
horizon, schedule expansion, and the malformed-entry errors.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.schema import ALLOWED_TAGS
from enrichment.normalize import normalize_record
from scrapers import curated_events as ce
from scrapers.base import ScraperError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str) -> None:
    """Point the scraper at a temporary curated_events.yaml."""
    path = tmp_path / "curated_events.yaml"
    path.write_text(body, encoding="utf-8")
    monkeypatch.setattr(ce, "CURATED_FILE", path)


def _fetch() -> list[dict]:
    return ce.CuratedEventsScraper().fetch_raw()


def _iso(days_from_today: int) -> str:
    return (dt.date.today() + dt.timedelta(days=days_from_today)).isoformat()


# ---------------------------------------------------------------------------
# Schedule expansion
# ---------------------------------------------------------------------------

def test_run_expands_to_one_record_per_day(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: test-run
    title: Test Run
    source_url: https://example.org/event
    venue: Test Venue
    run:
      start_date: {_iso(1)}
      end_date: {_iso(3)}
      start_time: "10:00"
      end_time: "16:00"
    verified_on: {_iso(0)}
""")
    records = _fetch()
    assert len(records) == 3
    assert [r["start_text"][:10] for r in records] == [_iso(1), _iso(2), _iso(3)]
    assert all(r["end_text"].endswith("16:00:00") for r in records)
    assert all(r["all_day"] is False for r in records)


def test_days_of_week_filters_the_run(tmp_path, monkeypatch):
    # A four-week window guarantees at least one of each weekday.
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: weekends-only
    title: Weekends Only
    source_url: https://example.org/event
    run:
      start_date: {_iso(1)}
      end_date: {_iso(28)}
      days_of_week: [sat, sun]
      start_time: "09:00"
    verified_on: {_iso(0)}
""")
    records = _fetch()
    assert records, "expected at least one weekend occurrence"
    for r in records:
        weekday = dt.date.fromisoformat(r["start_text"][:10]).weekday()
        assert weekday in (5, 6)


def test_explicit_dates_carry_per_day_hours_and_notes(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: fair
    title: Test Fair
    source_url: https://example.org/fair
    summary: A county fair.
    dates:
      - date: {_iso(2)}
        start_time: "17:00"
        end_time: "22:30"
      - date: {_iso(3)}
        start_time: "10:00"
        end_time: "22:30"
        note: "Sensory-friendly hours 10 a.m.-1 p.m."
    verified_on: {_iso(0)}
""")
    records = _fetch()
    assert len(records) == 2
    assert records[0]["start_text"].endswith("17:00:00")
    assert records[1]["start_text"].endswith("10:00:00")
    # The per-day note is appended to the shared summary, not substituted for it.
    assert records[0]["summary_text"] == "A county fair."
    assert "Sensory-friendly" in records[1]["summary_text"]
    assert records[1]["summary_text"].startswith("A county fair.")


def test_missing_start_time_produces_an_all_day_record(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: all-day
    title: All Day Run
    source_url: https://example.org/event
    run:
      start_date: {_iso(1)}
      end_date: {_iso(1)}
    verified_on: {_iso(0)}
""")
    (record,) = _fetch()
    assert record["all_day"] is True
    assert record["start_text"] == _iso(1)
    assert record["end_text"] is None


# ---------------------------------------------------------------------------
# Guardrails — the reason this source is safe to hand-maintain
# ---------------------------------------------------------------------------

def test_past_occurrences_are_dropped_but_today_is_kept(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: straddles-today
    title: Straddles Today
    source_url: https://example.org/event
    run:
      start_date: {_iso(-5)}
      end_date: {_iso(2)}
      start_time: "12:00"
    verified_on: {_iso(0)}
""")
    records = _fetch()
    dates = [r["start_text"][:10] for r in records]
    assert dates == [_iso(0), _iso(1), _iso(2)]


def test_a_fully_past_entry_publishes_nothing(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: last-year
    title: Last Year's Fair
    source_url: https://example.org/event
    run:
      start_date: {_iso(-400)}
      end_date: {_iso(-390)}
    verified_on: {_iso(-400)}
""")
    assert _fetch() == []


def test_occurrences_beyond_the_horizon_are_dropped(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: far-future
    title: Far Future Run
    source_url: https://example.org/event
    run:
      start_date: {_iso(1)}
      end_date: {_iso(ce.HORIZON_DAYS + 30)}
      start_time: "10:00"
    verified_on: {_iso(0)}
""")
    records = _fetch()
    horizon = dt.date.today() + dt.timedelta(days=ce.HORIZON_DAYS)
    assert records
    assert all(dt.date.fromisoformat(r["start_text"][:10]) <= horizon for r in records)


def test_stale_entry_is_warned_about_but_still_published(tmp_path, monkeypatch, caplog):
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: stale
    title: Stale Entry
    source_url: https://example.org/event
    run:
      start_date: {_iso(1)}
      end_date: {_iso(1)}
    verified_on: {_iso(-(ce.STALE_AFTER_DAYS + 10))}
""")
    with caplog.at_level("WARNING"):
        records = _fetch()
    assert len(records) == 1
    assert "last verified" in caplog.text


def test_disabled_entry_is_skipped(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: off
    title: Disabled Entry
    source_url: https://example.org/event
    enabled: false
    run:
      start_date: {_iso(1)}
      end_date: {_iso(1)}
    verified_on: {_iso(0)}
""")
    assert _fetch() == []


def test_missing_file_yields_no_events(tmp_path, monkeypatch):
    monkeypatch.setattr(ce, "CURATED_FILE", tmp_path / "does_not_exist.yaml")
    assert ce.CuratedEventsScraper().fetch_raw() == []


# ---------------------------------------------------------------------------
# Malformed entries fail loudly rather than publishing something wrong
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("body,expected", [
    ("""
events:
  - slug: no-url
    title: No Source URL
    run: {start_date: 2099-01-01, end_date: 2099-01-02}
""", "source_url"),
    ("""
events:
  - slug: no-schedule
    title: No Schedule
    source_url: https://example.org/e
""", "neither 'dates' nor 'run'"),
    ("""
events:
  - slug: both
    title: Both Schedules
    source_url: https://example.org/e
    dates: [{date: 2099-01-01}]
    run: {start_date: 2099-01-01, end_date: 2099-01-02}
""", "pick one"),
    ("""
events:
  - slug: backwards
    title: Backwards Run
    source_url: https://example.org/e
    run: {start_date: 2099-01-10, end_date: 2099-01-01}
""", "before run.start_date"),
    ("""
events:
  - slug: bad-day
    title: Bad Weekday
    source_url: https://example.org/e
    run: {start_date: 2099-01-01, end_date: 2099-01-02, days_of_week: [funday]}
""", "unknown day_of_week"),
])
def test_malformed_entries_raise(tmp_path, monkeypatch, body, expected):
    _write(tmp_path, monkeypatch, body)
    with pytest.raises(ScraperError) as exc:
        _fetch()
    assert expected in str(exc.value)


# ---------------------------------------------------------------------------
# End-to-end through normalization
# ---------------------------------------------------------------------------

def test_curated_record_normalizes_into_a_valid_event(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: e2e
    title: Test Circus
    organizer: Test Circus Co
    source_url: https://example.org/circus
    venue: Under the Big Top
    address: 8025 Galleria Drive, Tysons, VA 22102
    city: Tysons
    county: Fairfax
    summary: An international circus with acrobats and clowns.
    price_text: "Ticketed"
    tags: [circus, live_show, all_ages, outdoor]
    run:
      start_date: {_iso(1)}
      end_date: {_iso(1)}
      start_time: "19:00"
      end_time: "21:00"
    verified_on: {_iso(0)}
""")
    (record,) = _fetch()
    event = normalize_record(record)

    assert event is not None
    # Source-supplied facts survive normalization.
    assert event.source_name == "Test Circus Co"
    assert event.source_url == "https://example.org/circus"
    assert event.city == "Tysons"
    assert event.county == "Fairfax"
    assert event.location_name == "Under the Big Top"
    assert "8025 Galleria Drive" in (event.location_address or "")
    # Declared tags are honoured and remain within the allowed taxonomy.
    assert {"circus", "live_show", "all_ages", "outdoor"} <= set(event.tags)
    assert set(event.tags) <= ALLOWED_TAGS
    # "Ticketed" must not be read as free.
    assert event.is_free is False
    assert event.cost_type == "paid"


def test_declared_outdoor_beats_indoor_keyword_inference(tmp_path, monkeypatch):
    """
    A fair on community-center grounds is outdoors, even though both the venue
    name and the description read as indoor to the keyword rules.
    """
    _write(tmp_path, monkeypatch, f"""
events:
  - slug: fair-at-a-center
    title: Test County Fair
    source_url: https://example.org/fair
    venue: Thomas Jefferson Community Center
    summary: Indoor exhibits, a midway, and live entertainment.
    tags: [fair, outdoor, all_ages]
    dates:
      - date: {_iso(1)}
        start_time: "17:00"
    verified_on: {_iso(0)}
""")
    (record,) = _fetch()
    event = normalize_record(record)
    assert event is not None
    assert "outdoor" in event.tags
    assert "indoor" not in event.tags


def test_multi_day_run_survives_deduplication(tmp_path, monkeypatch):
    """
    Each day of a run must reach the published feed. The dedupe fingerprint is
    title + start DATE + location, so per-day expansion is what keeps a
    three-week circus stand from collapsing into a single entry.
    """
    from enrichment.dedupe import deduplicate

    _write(tmp_path, monkeypatch, f"""
events:
  - slug: multi-day
    title: Multi Day Run
    source_url: https://example.org/event
    venue: Some Venue
    run:
      start_date: {_iso(1)}
      end_date: {_iso(5)}
      start_time: "19:00"
    verified_on: {_iso(0)}
""")
    events = [normalize_record(r) for r in _fetch()]
    assert all(e is not None for e in events)
    assert len(deduplicate(events)) == 5
