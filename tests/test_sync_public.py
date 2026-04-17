"""
Tests for scripts/sync_public.py — the Vercel static-mirror syncer.

When retention prunes old weeks from data/published/events/, the mirror in
public/events/ must stay in lockstep; otherwise stale files linger on the
deployed site.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


@pytest.fixture
def sync_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Import scripts.sync_public with SRC/DST pointed at tmp dirs."""
    import scripts.sync_public as sync_public
    importlib.reload(sync_public)

    src = tmp_path / "data" / "published" / "events"
    dst = tmp_path / "public" / "events"
    src.mkdir(parents=True)
    dst.mkdir(parents=True)

    monkeypatch.setattr(sync_public, "SRC", src)
    monkeypatch.setattr(sync_public, "DST", dst)
    return sync_public, src, dst


class TestSyncPublic:
    def test_copies_files_to_destination(self, sync_module):
        mod, src, dst = sync_module
        (src / "index.json").write_text('{"version": "1"}')
        (src / "week-2026-03-16.json").write_text('{"events": []}')

        rc = mod.sync()
        assert rc == 0
        assert (dst / "index.json").exists()
        assert (dst / "week-2026-03-16.json").exists()

    def test_removes_orphaned_week_files(self, sync_module):
        """A stale week-*.json in public/events/ must be deleted when the
        source directory no longer contains it (retention pruned it)."""
        mod, src, dst = sync_module
        # Fresh source — three recent weeks
        (src / "index.json").write_text('{"version": "1"}')
        (src / "week-2026-03-09.json").write_text("{}")
        (src / "week-2026-03-16.json").write_text("{}")

        # Stale files still present in destination from a previous run
        (dst / "week-2025-12-29.json").write_text("{old}")
        (dst / "week-2026-01-05.json").write_text("{old}")
        (dst / "week-2026-03-09.json").write_text("{stale}")

        rc = mod.sync()
        assert rc == 0

        remaining = sorted(p.name for p in dst.glob("week-*.json"))
        assert remaining == ["week-2026-03-09.json", "week-2026-03-16.json"]

    def test_does_not_remove_non_week_files(self, sync_module):
        """Non-week files (like index.json or unrelated helpers) are left alone."""
        mod, src, dst = sync_module
        (src / "index.json").write_text('{"version": "1"}')

        # Destination has some unrelated file we don't own
        (dst / "README.md").write_text("docs")
        (dst / "week-2025-01-01.json").write_text("{old}")

        rc = mod.sync()
        assert rc == 0

        assert (dst / "README.md").exists()
        assert not (dst / "week-2025-01-01.json").exists()

    def test_returns_error_when_source_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        import scripts.sync_public as sync_public
        importlib.reload(sync_public)

        monkeypatch.setattr(sync_public, "SRC", tmp_path / "does-not-exist")
        monkeypatch.setattr(sync_public, "DST", tmp_path / "dst")

        assert sync_public.sync() == 1
