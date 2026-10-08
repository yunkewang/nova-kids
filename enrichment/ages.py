"""Age-range extraction from event titles and descriptions.

No scraper currently provides structured age_min/age_max, so the published
feed carries 0% age data — yet library/park listings constantly state ages
in free text ("Ages 3-5", "Grades K-2", "toddlers").  This module recovers
that signal with conservative, keyword-anchored patterns.

Priority: explicit age range > grade range > named age group.
Returns (age_min, age_max); either may be None when open-ended.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Explicit age ranges: "Ages 3-5", "ages 2 to 5", "age 6+", "3-5 years old"
# ---------------------------------------------------------------------------

_AGE_RANGE_RE = re.compile(
    r"\bages?\s*(\d{1,2})\s*(?:-|–|—|to|through|thru)\s*(\d{1,2})\b",
    re.IGNORECASE,
)
_AGE_PLUS_RE = re.compile(
    r"\bages?\s*(\d{1,2})\s*(?:\+|and\s+up|or\s+older)(?!\w)",
    re.IGNORECASE,
)
_YEARS_OLD_RE = re.compile(
    r"\b(\d{1,2})\s*(?:-|–|—|to|through)\s*(\d{1,2})\s*years?\s*old\b",
    re.IGNORECASE,
)
_MONTHS_RE = re.compile(
    r"\b(\d{1,2})\s*months?\s*(?:-|–|—|to|through)\s*(\d{1,2})\s*months?\b",
    re.IGNORECASE,
)
_MONTHS_TO_YEARS_RE = re.compile(
    r"\b(\d{1,2})\s*months?\s*(?:-|–|—|to|through)\s*(\d{1,2})\s*years?\b",
    re.IGNORECASE,
)
# "Ages 18-36 months" — must run BEFORE _AGE_RANGE_RE or "18-36" wins
_AGE_RANGE_MONTHS_RE = re.compile(
    r"\bages?\s*(\d{1,2})\s*(?:-|–|—|to|through)\s*(\d{1,2})\s*months?\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Grade ranges: "Grades K-2", "grade 3-5"  (K ~= age 5, grade N ~= age N+5)
# ---------------------------------------------------------------------------

_GRADE_RANGE_RE = re.compile(
    r"\bgrades?\s*([Kk0-9])\s*(?:-|–|—|to|through)\s*([Kk0-9])\b",
    re.IGNORECASE,
)
_GRADE_PLUS_RE = re.compile(
    r"\bgrades?\s*([Kk0-9])\s*(?:\+|and\s+up)\b",
    re.IGNORECASE,
)


def _grade_to_age(g: str) -> int:
    return 5 if g.upper() == "K" else int(g) + 5


# ---------------------------------------------------------------------------
# Named groups (lowest priority)
# ---------------------------------------------------------------------------

_NAMED_GROUPS: list[tuple[re.Pattern, tuple[int | None, int | None]]] = [
    (re.compile(r"\bbab(?:y|ies)\b|\binfants?\b", re.IGNORECASE), (0, 1)),
    (re.compile(r"\btoddlers?\b", re.IGNORECASE), (1, 3)),
    (re.compile(r"\bpreschool(?:ers?)?\b|\bpre-?k\b", re.IGNORECASE), (3, 5)),
    (re.compile(r"\bkindergarten(?:ers?)?\b", re.IGNORECASE), (5, 6)),
    (re.compile(r"\btweens?\b", re.IGNORECASE), (9, 12)),
    (re.compile(r"\bteens?\b|\bteenagers?\b|\bmiddle\s+school(?:ers?)?\b",
                re.IGNORECASE), (13, 17)),
    (re.compile(r"\belementary\b|\bschool[-\s]?age[sd]?\b", re.IGNORECASE), (5, 12)),
    (re.compile(r"\ball\s+ages\b", re.IGNORECASE), (0, 99)),
]


def _clamp(age: int | None) -> int | None:
    if age is None:
        return None
    return max(0, min(99, age))


def extract_age_range(title: str | None, summary: str | None
                      ) -> tuple[int | None, int | None]:
    """Extract (age_min, age_max) from free text.  (None, None) if unknown."""
    text = f"{title or ''} {summary or ''}"
    if not text.strip():
        return (None, None)

    m = _AGE_RANGE_MONTHS_RE.search(text)
    if m:
        return (_clamp(int(m.group(1)) // 12), _clamp(int(m.group(2)) // 12))

    m = _AGE_RANGE_RE.search(text)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        if lo <= hi:
            return (_clamp(lo), _clamp(hi))

    m = _AGE_PLUS_RE.search(text)
    if m:
        return (_clamp(int(m.group(1))), None)

    m = _YEARS_OLD_RE.search(text)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        if lo <= hi:
            return (_clamp(lo), _clamp(hi))

    m = _MONTHS_RE.search(text)
    if m:
        return (_clamp(int(m.group(1)) // 12), _clamp(int(m.group(2)) // 12))

    m = _MONTHS_TO_YEARS_RE.search(text)
    if m:
        return (_clamp(int(m.group(1)) // 12), _clamp(int(m.group(2))))

    m = _GRADE_RANGE_RE.search(text)
    if m:
        lo, hi = _grade_to_age(m.group(1)), _grade_to_age(m.group(2))
        if lo <= hi:
            return (lo, hi)

    m = _GRADE_PLUS_RE.search(text)
    if m:
        return (_grade_to_age(m.group(1)), None)

    for pattern, ages in _NAMED_GROUPS:
        if pattern.search(text):
            return ages

    return (None, None)
