"""
Tests for the two pipeline-level decisions that determine whether a run's
output reaches the feed at all.

Both were exposed by the 2026-08-15 run, which resolved 1600 DullesMoms
candidates and published 1993 events — none of which shipped:

  1. The published week was inferred from the earliest event, so one stale
     seed date sent the whole batch to week-2026-05-25.json.
  2. Nothing dropped events that had already finished, which is what put a
     May date in an August batch in the first place.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from config.schema import Event
from scripts.run_pipeline import _drop_past_events


def _event(title: str, start: datetime, end: datetime | None = None) -> Event:
    return Event(
        id=f"{abs(hash((title, start))):016x}"[:16],
        title=title,
        start=start,
        end=end,
        source_name="Test Source",
        source_url="https://example.org/events/1",
        last_verified_at=datetime.now(tz=timezone.utc),
    )


# ---------------------------------------------------------------------------
# _drop_past_events
# ---------------------------------------------------------------------------

class TestDropPastEvents:
    TODAY = date(2026, 8, 15)

    def test_finished_events_are_dropped(self):
        stale = _event("Spring Fling", datetime(2026, 5, 30, 10, 0))
        fresh = _event("Pumpkin Harvest", datetime(2026, 9, 12, 10, 0))

        upcoming, finished = _drop_past_events([stale, fresh], self.TODAY)

        assert [e.title for e in upcoming] == ["Pumpkin Harvest"]
        assert [e.title for e in finished] == ["Spring Fling"]

    def test_todays_events_are_kept(self):
        """An event earlier today is still worth showing for the rest of it."""
        today = _event("Morning Storytime", datetime(2026, 8, 15, 9, 30))
        upcoming, finished = _drop_past_events([today], self.TODAY)
        assert upcoming == [today]
        assert finished == []

    def test_multi_day_event_survives_until_its_last_day(self):
        fair = _event(
            "County Fair",
            datetime(2026, 8, 10, 10, 0),
            end=datetime(2026, 8, 20, 22, 0),
        )
        upcoming, finished = _drop_past_events([fair], self.TODAY)
        assert upcoming == [fair]
        assert finished == []

    def test_multi_day_event_is_dropped_once_it_ends(self):
        fair = _event(
            "County Fair",
            datetime(2026, 7, 10, 10, 0),
            end=datetime(2026, 7, 20, 22, 0),
        )
        upcoming, finished = _drop_past_events([fair], self.TODAY)
        assert upcoming == []
        assert finished == [fair]

    def test_empty_input(self):
        assert _drop_past_events([], self.TODAY) == ([], [])


# ---------------------------------------------------------------------------
# Publish week anchoring
# ---------------------------------------------------------------------------

class TestPublishWeekAnchor:
    """The published filename must not be a function of the scraped data."""

    @pytest.fixture
    def published_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        scratch = tmp_path / "published" / "events"
        scratch.mkdir(parents=True)
        import config.settings as settings
        import enrichment.publish as publish
        monkeypatch.setattr(settings, "PUBLISHED_DIR", scratch)
        monkeypatch.setattr(publish, "PUBLISHED_DIR", scratch)
        return scratch

    def test_explicit_week_beats_a_stale_earliest_event(self, published_dir: Path):
        from enrichment.publish import publish_events

        events = [
            _event("Stale Listing", datetime(2026, 5, 30, 10, 0)),
            _event("Pumpkin Harvest", datetime(2026, 9, 12, 10, 0)),
        ]
        result = publish_events(events, week_start=date(2026, 8, 15))

        assert result.output_path.name == "week-2026-08-10.json"
        assert result.event_count == 2

    def test_inference_still_follows_the_earliest_event(self, published_dir: Path):
        """Unchanged behaviour for callers that do not name a week."""
        from enrichment.publish import publish_events

        soon = datetime.now() + timedelta(days=3)
        later = datetime.now() + timedelta(days=30)
        result = publish_events([_event("A", soon), _event("B", later)])

        expected = (soon.date() - timedelta(days=soon.weekday())).isoformat()
        assert result.output_path.name == f"week-{expected}.json"

    def test_publishing_an_old_week_keeps_the_file(
        self, published_dir: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """End-to-end guard against the 2026-08-15 self-deletion."""
        import enrichment.publish as publish
        monkeypatch.setattr(publish, "MAX_PUBLISHED_WEEKS", 5)

        for week in ("2026-06-22", "2026-06-29", "2026-07-27",
                     "2026-08-03", "2026-08-10"):
            (published_dir / f"week-{week}.json").write_text(
                '{"week_start": "%s", "event_count": 0, "events": []}' % week
            )

        result = publish.publish_events(
            [_event("Stale Listing", datetime(2026, 5, 30, 10, 0))],
            week_start=date(2026, 5, 25),
        )

        assert result.output_path.exists(), (
            "publish deleted the week file it had just written"
        )
        assert result.output_path.name == "week-2026-05-25.json"
