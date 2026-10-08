"""Tests for age-range extraction from titles/descriptions (2026-10-07).

The published feed carried 0% age data because no scraper provides
structured ages.  This module recovers the signal from free text.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from enrichment.ages import extract_age_range


def test_explicit_age_range():
    assert extract_age_range("Storytime Ages 3-5", None) == (3, 5)
    assert extract_age_range("Crafts for ages 2 to 5", "") == (2, 5)
    assert extract_age_range("Music", "Ages 6–8 welcome") == (6, 8)


def test_age_plus():
    assert extract_age_range("LEGO Club age 6+", None) == (6, None)
    assert extract_age_range("Ages 13 and up", None) == (13, None)


def test_years_old_phrase():
    assert extract_age_range("Playgroup 2-4 years old", None) == (2, 4)


def test_months():
    # 18-36 months -> (1, 3)
    assert extract_age_range("Baby bounce 6-18 months", None) == (0, 1)
    assert extract_age_range("Storytime for Ages 18-36 months", None) == (1, 3)


def test_grade_ranges():
    # K ~= 5, grade N ~= N+5
    assert extract_age_range("STEAM Grades K-2", None) == (5, 7)
    assert extract_age_range("Chess club grades 3-5", None) == (8, 10)
    assert extract_age_range("Grades 6 and up", None) == (11, None)


def test_named_groups():
    assert extract_age_range("Toddler dance", None) == (1, 3)
    assert extract_age_range("Preschool art", None) == (3, 5)
    assert extract_age_range("Teen anime club", None) == (13, 17)
    assert extract_age_range("Family festival, all ages", None) == (0, 99)


def test_no_false_positives():
    assert extract_age_range("Fall Festival 2026", "Join us Saturday!") == (None, None)
    assert extract_age_range("5K Fun Run", None) == (None, None)
    assert extract_age_range(None, None) == (None, None)


def test_priority_explicit_over_named():
    # "Ages 3-5" wins over the word "teens" elsewhere in the text
    assert extract_age_range("Ages 3-5", "siblings and teens welcome") == (3, 5)
