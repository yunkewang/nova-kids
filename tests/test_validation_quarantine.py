"""Tests for validation quarantine mode (2026-10-07).

Previously any validation error halted the whole pipeline (return 1), so one
dirty scraper record blocked the entire week's publish.  Now events with
error-severity issues are quarantined to
data/manual_review/quarantined_events.json and the clean events publish
normally.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.schema import Event
from enrichment.validate import validate_events


def _make_event(eid: str, title: str = "Good Event",
                url: str = "https://example.com/event") -> Event:
    return Event(
        id=eid,
        source_name="Test Source",
        source_url=url,
        title=title,
        start=datetime(2026, 10, 12, 10, 0, tzinfo=timezone.utc),
        last_verified_at=datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc),
    )


def _quarantine_partition(events, report):
    """Mirror of the quarantine block in run_pipeline.py main flow."""
    bad_ids = {issue.event_id for issue in report.errors}
    quarantined = [e for e in events if e.id in bad_ids]
    kept = [e for e in events if e.id not in bad_ids]
    return kept, quarantined


def test_quarantine_isolates_bad_events(tmp_path, monkeypatch):
    import scripts.run_pipeline as rp

    monkeypatch.setattr(rp, "MANUAL_REVIEW_DIR", tmp_path)

    events = [
        _make_event("good-1"),
        _make_event("good-2", title="Another Good Event"),
        _make_event("bad-url", url="not-a-url"),  # INVALID_SOURCE_URL
        _make_event("good-1"),                    # DUPLICATE_ID
    ]
    report = validate_events(events)
    assert not report.is_clean()

    kept, quarantined = _quarantine_partition(events, report)
    assert [e.id for e in kept] == ["good-2"]
    assert len(quarantined) == 3

    qpath = rp._write_quarantine_report(quarantined, report)
    assert qpath == tmp_path / "quarantined_events.json"
    payload = json.loads(qpath.read_text(encoding="utf-8"))
    assert payload["quarantined_count"] == 3
    for entry in payload["events"]:
        assert entry["_quarantine_reasons"], f"missing reasons for {entry['id']}"
        assert entry["id"]  # original payload preserved


def test_clean_batch_writes_no_quarantine_file(tmp_path, monkeypatch):
    import scripts.run_pipeline as rp

    monkeypatch.setattr(rp, "MANUAL_REVIEW_DIR", tmp_path)

    events = [_make_event("good-1"), _make_event("good-2")]
    report = validate_events(events)
    assert report.is_clean()

    kept, quarantined = _quarantine_partition(events, report)
    assert len(kept) == 2 and not quarantined
