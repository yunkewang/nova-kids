"""
Annotation layer — generates derived short_note for publishable events.

RULES (non-negotiable):
  - short_note must be a single sentence, max 200 characters.
  - It may only describe facts that were explicitly extracted from the original
    source page and are present as structured fields on the event dict.
  - It must NOT paraphrase, summarize, or reference DullesMoms content.
  - It must NOT invent pricing, age ranges, amenities, or any other details.
  - If the available facts are insufficient for a meaningful sentence, return None.

This module is purely template-driven — it does not call any external AI API.
Tags, cost, venue, and age fields are the only inputs.

Public entry point: generate_short_note(event_data: dict) -> str | None
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any


# ---------------------------------------------------------------------------
# Individual fact extractors
# ---------------------------------------------------------------------------

def _cost_phrase(event_data: dict[str, Any]) -> str | None:
    """Return a brief cost phrase, or None if cost is unknown."""
    cost = event_data.get("cost_type", "unknown")
    # Handle both enum value and string
    cost_str = cost.value if hasattr(cost, "value") else str(cost)
    if cost_str == "free":
        return "Free"
    if cost_str == "paid":
        price_text = event_data.get("price_text")
        if price_text:
            # Keep it short
            return price_text[:40].strip() if len(price_text) > 40 else price_text
        return "Paid"
    if cost_str == "suggested_donation":
        return "Suggested donation"
    return None  # unknown — don't guess


def _setting_phrase(tags: list[str]) -> str | None:
    """Return 'indoor' or 'outdoor' if clearly tagged."""
    if "indoor" in tags and "outdoor" not in tags:
        return "indoor"
    if "outdoor" in tags and "indoor" not in tags:
        return "outdoor"
    if "virtual" in tags:
        return "online/virtual"
    return None


def _age_phrase(event_data: dict[str, Any]) -> str | None:
    """Return an age phrase only when explicit age fields are present."""
    age_min = event_data.get("age_min")
    age_max = event_data.get("age_max")
    tags = event_data.get("tags", [])

    if age_min is not None and age_max is not None:
        if age_min == 0 and age_max <= 3:
            return "for infants and toddlers"
        if age_min == 0:
            return f"for ages {age_min}–{age_max}"
        return f"for ages {age_min}–{age_max}"
    if age_min is not None:
        return f"for ages {age_min}+"
    if age_max is not None:
        return f"for ages up to {age_max}"

    # Fall back to broad age tags only
    if "toddler" in tags and "preschool" not in tags and "elementary" not in tags:
        return "for toddlers"
    if "all_ages" in tags:
        return "for all ages"
    return None


def _venue_phrase(event_data: dict[str, Any]) -> str | None:
    """Return 'at <venue>' phrase if location is known."""
    location = event_data.get("location_name")
    # Don't say "at Virtual" — setting phrase already covers this
    if location and location.lower().strip() not in ("virtual", "online") and len(location) < 60:
        return f"at {location}"
    city = event_data.get("city")
    county = event_data.get("county")
    if city:
        return f"in {city}"
    if county:
        return f"in {county} County"
    return None


def _activity_phrase(tags: list[str], title: str = "") -> str | None:
    """Return a brief activity description from tags (and title for specificity)."""
    title_lower = title.lower()

    # Sports: be more specific when title gives us context
    if "sports" in tags:
        if any(kw in title_lower for kw in ("skate", "skating", "ice", "rink")):
            return "skating session"
        if "swim" in tags or any(kw in title_lower for kw in ("swim", "pool", "aquatic")):
            return "swim session"

    # Priority-ordered activity descriptors
    activity_map = [
        ("storytime", "storytime"),
        ("stem", "STEM activity"),
        ("arts", "arts program"),
        ("crafts", "craft activity"),
        ("music", "music program"),
        ("theater", "theater performance"),
        ("animals", "animals program"),
        ("nature", "nature program"),
        ("sports", "sports activity"),
        ("swim", "swim program"),
        ("hiking", "hike"),
        ("cooking", "cooking class"),
        ("fitness", "fitness class"),
        ("workshop", "workshop"),
        ("camp", "camp program"),
        ("festival", "festival"),
        ("holiday", "holiday event"),
    ]
    for tag, phrase in activity_map:
        if tag in tags:
            return phrase
    return None


# ---------------------------------------------------------------------------
# Note assembly
# ---------------------------------------------------------------------------

_MAX_NOTE_LEN = 200


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+[A-Z]")

# Venue homepage boilerplate openers — these are marketing copy, not event notes.
# If a summary starts with one of these, skip the fast-path and use the template.
_BOILERPLATE_OPENER_RE = re.compile(
    r"^(?:"
    r"visit\s+(?:our|the)\s"
    r"|explore\s+(?:our|the)\s"
    r"|explore\s+award"
    r"|savor\s+"
    r"|indulge\s+"
    r"|just\s+minutes\s+from"
    r"|eat,?\s+drink"
    r"|dulles\s+sportsplex\s+is"
    r"|the\s+(?:alden|charles|cascades|rust|brambleton|ashburn|sterling|udvar)\s+\w+\s+is"
    r"|the\s+sterling\s+community\s+center\s+offers"
    r"|the\s+claude\s+moore\s+\w+\s+is"
    r"|port\s+discovery,?\s+located"
    r"|a\s+toy\s+library"
    r"|visit\s+our\s+"
    r"|explore\s+our\s+"
    r"|our\s+\w[\w\s]{2,20}\s+(?:school|center|studio)\s+\w"
    r"|since\s+\d{4},?\s"
    r"|all\s+in\s+for\s+animals"
    r"|children\s+register\b"
    r"|learn\s+more\s+at\s+(?:www\.|https?://)"
    r"|fairfax\s+county,?\s+virginia\s*-"
    r"|live\s+webinar\s+\w+\s+\d{1,2}"
    r"|tackett.?s\s+mill\s+center"
    r"|the\s+(?:claude\s+moore|alden\s+theatre?|charles\s+houston)\s+\w"
    r"|claude\s+moore\s+(?:rec(?:reation)?|park)\s+"
    r"|madison\s+features?\s+a\s+\w"
    r"|dulles\s+south\s+rec\s+and"
    r"|franklin\s+park\s+is\s+a\b"
    r"|[a-z][\w\s'.-]{5,50}\s+is\s+a\s+beautiful\b"
    r")",
    re.IGNORECASE,
)


def _clean_summary(text: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    text = _HTML_TAG_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def _is_boilerplate_summary(text: str) -> bool:
    """Return True if text looks like venue marketing copy rather than event description."""
    return bool(_BOILERPLATE_OPENER_RE.match(text.strip()))


def generate_short_note(event_data: dict[str, Any]) -> str | None:
    """
    Compose a single-sentence note from structured event facts only.

    Fast-path: if the event has a clean summary that is ≤120 chars, a single
    sentence, and does not look like venue homepage boilerplate, use it directly.

    Fallback template: "[Cost] [setting] [activity] [venue_phrase] [age_phrase]."

    Returns None when there are insufficient facts for a meaningful sentence.
    Does not invent or guess any details.
    """
    # Fast-path: clean summary ≤120 chars, single sentence, not venue boilerplate
    summary_raw = event_data.get("summary") or ""
    if summary_raw:
        summary_clean = _clean_summary(summary_raw)
        if (
            summary_clean
            and len(summary_clean) <= 120
            and not _MULTI_SENTENCE_RE.search(summary_clean)
            and not _is_boilerplate_summary(summary_clean)
        ):
            if not summary_clean.endswith((".", "!", "?")):
                summary_clean += "."
            return summary_clean

    tags = event_data.get("tags", [])

    cost = _cost_phrase(event_data)
    setting = _setting_phrase(tags)
    activity = _activity_phrase(tags, title=event_data.get("title") or "")
    venue = _venue_phrase(event_data)
    age = _age_phrase(event_data)

    # We need at least one of (activity, venue) for the note to say anything
    if not activity and not venue:
        return None

    # Build parts
    parts: list[str] = []

    # Lead with cost as a differentiator (e.g. "Free indoor storytime…")
    if cost:
        parts.append(cost)

    if setting:
        parts.append(setting)

    if activity:
        parts.append(activity)
    elif venue:
        # If no activity tag, prefix with "family-friendly event" for context
        parts.append("family-friendly event")

    if venue:
        parts.append(venue)

    if age:
        parts.append(age)

    if not parts:
        return None

    # Assemble sentence
    sentence = " ".join(parts)
    sentence = sentence[0].upper() + sentence[1:]
    if not sentence.endswith("."):
        sentence += "."

    # Truncate safely if somehow over limit (shouldn't happen normally)
    if len(sentence) > _MAX_NOTE_LEN:
        sentence = sentence[: _MAX_NOTE_LEN - 1].rstrip() + "."

    return sentence


# ---------------------------------------------------------------------------
# Multi-sentence guard
# ---------------------------------------------------------------------------

_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")


def validate_short_note(note: str | None) -> tuple[bool, str]:
    """
    Check a short_note for policy compliance.

    Returns (is_valid, reason_if_invalid).
    """
    if note is None:
        return True, ""
    if len(note) > _MAX_NOTE_LEN:
        return False, f"short_note exceeds {_MAX_NOTE_LEN} chars ({len(note)})"
    if len(_SENTENCE_BOUNDARY.findall(note)) > 0:
        return False, "short_note appears to contain multiple sentences"
    return True, ""
