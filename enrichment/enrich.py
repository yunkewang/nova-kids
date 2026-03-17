"""
Enrichment functions — derive tags, scores, and flags from normalized event data.

These functions operate on plain dicts (before Pydantic validation) so that
enrichment results can be validated together with the rest of the event.

Public entry point: enrich_event(event_data: dict) -> dict
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from config.known_venues import lookup_venue_multi
from config.schema import ALLOWED_TAGS, CostType

# ---------------------------------------------------------------------------
# Keyword → tag mapping
# ---------------------------------------------------------------------------

# Each entry: (tag, list-of-regex-patterns-to-search-in-title+summary)
_TAG_RULES: list[tuple[str, list[str]]] = [
    # Setting
    ("indoor",     [r"\bindoor\b", r"\binside\b", r"\bmuseum\b", r"\blibrary\b",
                    r"\bcommunity center\b", r"\bpublic library\b"]),
    ("outdoor",    [r"\boutdoor\b", r"\boutside\b", r"\bpark\b", r"\btrail\b",
                    r"\bnature center\b", r"\bgarden\b", r"\bplayground\b"]),
    ("virtual",    [r"\bvirtual\b", r"\bonline\b", r"\bzoom\b", r"\bwebinar\b"]),
    # Activity types
    ("storytime",  [r"\bstory\s*time\b", r"\bread\s*aloud\b", r"\bstories\b"]),
    ("stem",       [r"\bstem\b", r"\bscience\b", r"\btechnology\b", r"\bengineering\b",
                    r"\bmath\b", r"\brobotics\b", r"\bcoding\b"]),
    ("arts",       [r"\bart\b", r"\bdrawing\b", r"\bpainting\b", r"\bcraft\b",
                    r"\bceramics\b", r"\bsketch\b"]),
    ("crafts",     [r"\bcraft\b", r"\bmaking\b", r"\bdiy\b", r"\bbuilding\b"]),
    ("music",      [r"\bmusic\b", r"\bconcert\b", r"\bsinging\b", r"\bband\b",
                    r"\borchestra\b", r"\bchoir\b"]),
    ("theater",    [r"\btheater\b", r"\btheatre\b", r"\bplay\b", r"\bpuppet\b",
                    r"\bperformance\b", r"\bshow\b"]),
    ("sports",     [r"\bsports?\b", r"\bsoccer\b", r"\bbasketball\b", r"\btennis\b",
                    r"\bswimming\b", r"\bgymnastics\b", r"\bkickball\b"]),
    ("swim",       [r"\bswim\b", r"\bpool\b", r"\baquatic\b"]),
    ("hiking",     [r"\bhike\b", r"\bhiking\b", r"\bwalk\b", r"\btrail\b"]),
    ("nature",     [r"\bnature\b", r"\bwildlife\b", r"\bbird\b", r"\bplant\b",
                    r"\bforest\b", r"\becology\b"]),
    ("cooking",    [r"\bcook\b", r"\bcooking\b", r"\bculinary\b", r"\bbaking\b",
                    r"\bfood\b"]),
    ("fitness",    [r"\bfitness\b", r"\bexercise\b", r"\byoga\b", r"\bmovement\b"]),
    ("workshop",   [r"\bworkshop\b", r"\bclass\b", r"\bprogram\b", r"\bsession\b"]),
    ("camp",       [r"\bcamp\b", r"\bsummer camp\b", r"\bday camp\b"]),
    ("festival",   [r"\bfestival\b", r"\bfair\b", r"\bcelebration\b"]),
    ("holiday",    [r"\bholiday\b", r"\bhalloween\b", r"\bthanksgiving\b",
                    r"\bchristmas\b", r"\bhanukkah\b", r"\beaster\b",
                    r"\bvalentine\b", r"\bpresidents\b"]),
    # Age groups
    ("toddler",    [r"\btoddler\b", r"\bbaby\b", r"\binfant\b", r"\blap sit\b",
                    r"\bages?\s*0", r"\bages?\s*1\b", r"\bages?\s*2\b",
                    r"\bages?\s*3\b"]),
    ("preschool",  [r"\bpreschool\b", r"\bpre-?k\b", r"\bages?\s*[34]\b"]),
    ("elementary", [r"\belementary\b", r"\bschool.?age\b", r"\bkids?\b",
                    r"\bchildren\b", r"\bages?\s*[5-9]\b", r"\bages?\s*1[012]\b"]),
    ("teen",       [r"\bteen\b", r"\btween\b", r"\byouth\b", r"\bjunior\b",
                    r"\bages?\s*1[3-9]\b"]),
    ("all_ages",   [r"\ball ages?\b", r"\beveryone\b", r"\bfamilies\b",
                    r"\bfamily.?friendly\b"]),
]


def _search_text(patterns: list[str], text: str) -> bool:
    """Return True if any pattern matches text (case-insensitive)."""
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def derive_tags(event_data: dict[str, Any]) -> list[str]:
    """
    Derive classification tags from event title, summary, and other fields.

    Only returns tags in ALLOWED_TAGS.
    """
    search_text = " ".join(
        filter(
            None,
            [
                event_data.get("title", ""),
                event_data.get("summary", ""),
                event_data.get("location_name", ""),
            ],
        )
    )

    tags: set[str] = set()

    for tag, patterns in _TAG_RULES:
        if _search_text(patterns, search_text):
            tags.add(tag)

    # Cost-based tag
    if event_data.get("cost_type") == CostType.FREE or event_data.get("cost_type") == "free":
        tags.add("free")

    # Time-based tags
    start: datetime | None = event_data.get("start")
    if isinstance(start, datetime):
        weekday = start.weekday()  # 0=Mon, 6=Sun
        if weekday >= 5:
            tags.add("weekend")
        else:
            tags.add("weekday")

        hour = start.hour
        if 5 <= hour < 12:
            tags.add("morning")
        elif 12 <= hour < 17:
            tags.add("afternoon")
        elif 17 <= hour < 22:
            tags.add("evening")

    # Filter to allowed tags only (belt-and-suspenders; schema validator will also check)
    return sorted(tags & ALLOWED_TAGS)


def derive_rainy_day_friendly(tags: list[str]) -> bool:
    """
    True when the event is suitable regardless of weather.

    Indoor and virtual events qualify; outdoor-only events do not.
    """
    tag_set = set(tags)
    if "virtual" in tag_set:
        return True
    if "indoor" in tag_set and "outdoor" not in tag_set:
        return True
    return False


def compute_family_friendly_score(event_data: dict[str, Any], tags: list[str]) -> float:
    """
    Compute a 0–1 family-friendliness score.

    Heuristic weights:
      +0.30  tagged all_ages, kids, elementary, toddler, or preschool
      +0.15  free event
      +0.10  indoor
      +0.10  storytime, arts, crafts, stem, nature, music
      +0.10  weekend
      +0.05  rainy_day_friendly
      +0.05  has summary
      +0.05  has image_url
      +0.05  has registration_url
      -0.10  teen-only (without all_ages or elementary)
    Total possible: ~1.05 → capped at 1.0
    """
    score = 0.0
    tag_set = set(tags)

    # Audience
    family_age_tags = {"all_ages", "toddler", "preschool", "elementary"}
    if tag_set & family_age_tags:
        score += 0.30
    elif "teen" in tag_set:
        score -= 0.10  # teen-only is less broadly family-friendly

    # Cost
    if "free" in tag_set:
        score += 0.15

    # Setting
    if "indoor" in tag_set:
        score += 0.10

    # Activity richness
    enriching_tags = {"storytime", "arts", "crafts", "stem", "nature", "music",
                      "theater", "cooking", "workshop"}
    if tag_set & enriching_tags:
        score += 0.10

    # Timing
    if "weekend" in tag_set:
        score += 0.10

    # Weather
    if derive_rainy_day_friendly(tags):
        score += 0.05

    # Completeness bonuses
    if event_data.get("summary"):
        score += 0.05
    if event_data.get("image_url"):
        score += 0.05
    if event_data.get("registration_url"):
        score += 0.05

    return round(min(max(score, 0.0), 1.0), 4)


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def enrich_event(event_data: dict[str, Any]) -> dict[str, Any]:
    """
    Mutate and return event_data with derived tags, score, and flags.

    Called by normalize.normalize_record() before Pydantic validation.
    """
    tags = derive_tags(event_data)
    rainy_day = derive_rainy_day_friendly(tags)

    # Apply known-venue overrides (merge tags; override rainy_day / city / county)
    venue_hint = lookup_venue_multi(
        event_data.get("location_name"),
        event_data.get("title"),
        event_data.get("source_url"),
    )
    if venue_hint:
        # Merge hint tags into derived tags
        extra_tags = [t for t in venue_hint.get("tags", []) if t in ALLOWED_TAGS]
        tags = sorted(set(tags) | set(extra_tags))
        # Override rainy_day only when the hint explicitly sets it
        if "rainy_day_friendly" in venue_hint:
            rainy_day = venue_hint["rainy_day_friendly"]
        else:
            rainy_day = derive_rainy_day_friendly(tags)
        # Fill city / county only when missing
        if not event_data.get("city") and venue_hint.get("city"):
            event_data["city"] = venue_hint["city"]
        if not event_data.get("county") and venue_hint.get("county"):
            event_data["county"] = venue_hint["county"]

    score = compute_family_friendly_score(event_data, tags)

    event_data["tags"] = tags
    event_data["rainy_day_friendly"] = rainy_day
    event_data["family_friendly_score"] = score

    return event_data
