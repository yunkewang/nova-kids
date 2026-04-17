"""
Tests for the weekly publish + retention logic in enrichment/publish.py.

The retention policy keeps only the most recent MAX_PUBLISHED_WEEKS of
`week-YYYY-MM-DD.json` files under `data/published/events/`. Older files
are deleted on every publish run so the live feed never grows unbounded.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest


@pytest.fixture
def tmp_published_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect PUBLISHED_DIR (and the enrichment.publish module's view of it)
    to a scratch directory for the duration of the test."""
    scratch = tmp_path / "published" / "events"
    scratch.mkdir(parents=True, exist_ok=True)

    import config.settings as settings
    import enrichment.publish as publish

    monkeypatch.setattr(settings, "PUBLISHED_DIR", scratch)
    monkeypatch.setattr(publish, "PUBLISHED_DIR", scratch)
    return scratch


def _touch_week(dir_: Path, week_start: str, event_count: int = 100) -> Path:
    """Write a minimal week-*.json file for retention-prune testing."""
    path = dir_ / f"week-{week_start}.json"
    payload = {
        "week_start": week_start,
        "generated_at": "2026-04-16T10:00:00+00:00",
        "source_count": 1,
        "event_count": event_count,
        "events": [],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# _prune_old_weeks — pure function
# ---------------------------------------------------------------------------

class TestPruneOldWeeks:
    def test_keeps_all_when_under_limit(self, tmp_published_dir: Path):
        from enrichment.publish import _prune_old_weeks

        weeks = ["2026-03-02", "2026-03-09", "2026-03-16"]
        for w in weeks:
            _touch_week(tmp_published_dir, w)

        kept, removed = _prune_old_weeks(weeks, max_weeks=5)
        assert kept == weeks
        assert removed == []
        # All files still present
        assert sorted(p.name for p in tmp_published_dir.glob("week-*.json")) == [
            "week-2026-03-02.json",
            "week-2026-03-09.json",
            "week-2026-03-16.json",
        ]

    def test_prunes_oldest_over_limit(self, tmp_published_dir: Path):
        from enrichment.publish import _prune_old_weeks

        weeks = [
            "2026-02-02", "2026-02-09", "2026-02-16", "2026-02-23",
            "2026-03-02", "2026-03-09", "2026-03-16",
        ]
        for w in weeks:
            _touch_week(tmp_published_dir, w)

        kept, removed = _prune_old_weeks(weeks, max_weeks=5)

        # Keeps the 5 newest
        assert kept == [
            "2026-02-16", "2026-02-23",
            "2026-03-02", "2026-03-09", "2026-03-16",
        ]
        # Removes the oldest
        assert removed == ["2026-02-02", "2026-02-09"]

        # Physical files removed
        remaining = sorted(p.name for p in tmp_published_dir.glob("week-*.json"))
        assert "week-2026-02-02.json" not in remaining
        assert "week-2026-02-09.json" not in remaining
        assert "week-2026-03-16.json" in remaining

    def test_max_weeks_zero_disables_pruning(self, tmp_published_dir: Path):
        from enrichment.publish import _prune_old_weeks

        weeks = ["2025-01-06", "2025-01-13", "2025-01-20"]
        for w in weeks:
            _touch_week(tmp_published_dir, w)

        kept, removed = _prune_old_weeks(weeks, max_weeks=0)
        assert sorted(kept) == weeks
        assert removed == []
        assert len(list(tmp_published_dir.glob("week-*.json"))) == 3

    def test_deduplicates_input(self, tmp_published_dir: Path):
        from enrichment.publish import _prune_old_weeks

        weeks = ["2026-03-02", "2026-03-02", "2026-03-09"]
        for w in set(weeks):
            _touch_week(tmp_published_dir, w)

        kept, removed = _prune_old_weeks(weeks, max_weeks=5)
        assert kept == ["2026-03-02", "2026-03-09"]
        assert removed == []

    def test_tolerates_missing_files(self, tmp_published_dir: Path):
        """Pruning a week whose file is already gone shouldn't raise."""
        from enrichment.publish import _prune_old_weeks

        weeks = ["2026-01-05", "2026-01-12", "2026-03-16"]
        _touch_week(tmp_published_dir, "2026-03-16")  # only newest exists

        kept, removed = _prune_old_weeks(weeks, max_weeks=1)
        assert kept == ["2026-03-16"]
        assert set(removed) == {"2026-01-05", "2026-01-12"}


# ---------------------------------------------------------------------------
# publish_events — end-to-end retention
# ---------------------------------------------------------------------------

class TestPublishEventsRetention:
    def test_publish_prunes_older_than_cap(
        self, tmp_published_dir: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """After publish, only MAX_PUBLISHED_WEEKS files should remain."""
        import enrichment.publish as publish
        monkeypatch.setattr(publish, "MAX_PUBLISHED_WEEKS", 3)

        # Seed 6 pre-existing weeks on disk
        seed_weeks = [
            "2026-02-02", "2026-02-09", "2026-02-16",
            "2026-02-23", "2026-03-02", "2026-03-09",
        ]
        for w in seed_weeks:
            _touch_week(tmp_published_dir, w)

        # Publish a new week (2026-03-16)
        new_week = date(2026, 3, 16)
        result = publish.publish_events([], week_start=new_week)

        assert result.week_start == new_week

        remaining = sorted(p.name for p in tmp_published_dir.glob("week-*.json"))
        # Max 3 retained (2026-03-02, 2026-03-09, 2026-03-16)
        assert remaining == [
            "week-2026-03-02.json",
            "week-2026-03-09.json",
            "week-2026-03-16.json",
        ]

        index = json.loads((tmp_published_dir / "index.json").read_text())
        assert index["available_weeks"] == [
            "2026-03-02", "2026-03-09", "2026-03-16",
        ]
        assert index["latest_week"] == "2026-03-16"

    def test_publish_under_cap_keeps_everything(
        self, tmp_published_dir: Path, monkeypatch: pytest.MonkeyPatch
    ):
        import enrichment.publish as publish
        monkeypatch.setattr(publish, "MAX_PUBLISHED_WEEKS", 5)

        _touch_week(tmp_published_dir, "2026-03-09")
        result = publish.publish_events([], week_start=date(2026, 3, 16))

        assert result.week_start == date(2026, 3, 16)
        files = sorted(p.name for p in tmp_published_dir.glob("week-*.json"))
        assert files == ["week-2026-03-09.json", "week-2026-03-16.json"]

    def test_publish_preserves_current_week_even_with_very_small_cap(
        self, tmp_published_dir: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Even with max_weeks=1, the week we're publishing must survive."""
        import enrichment.publish as publish
        monkeypatch.setattr(publish, "MAX_PUBLISHED_WEEKS", 1)

        for w in ["2026-03-02", "2026-03-09"]:
            _touch_week(tmp_published_dir, w)

        publish.publish_events([], week_start=date(2026, 3, 16))

        files = sorted(p.name for p in tmp_published_dir.glob("week-*.json"))
        assert files == ["week-2026-03-16.json"]
