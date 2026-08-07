"""
Tests for the expanded activity-type taxonomy and the keyword rules behind it.

Two things are covered:
  1. The new outing tags (circus, fair, farm, lights, …) fire on realistic text.
  2. The loosened rules they replaced no longer produce their old false
     positives — an "online" ticket note must not make a physical venue
     virtual, and "fair food" must not make an event a cooking class.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.schema import ALLOWED_TAGS
from enrichment.enrich import derive_tags


def _tags(title: str, summary: str = "", location: str = "") -> set[str]:
    return set(derive_tags({
        "title": title,
        "summary": summary,
        "location_name": location,
        "start": datetime(2026, 8, 15, 14, 0),
    }))


# ---------------------------------------------------------------------------
# New tags exist and are reachable
# ---------------------------------------------------------------------------

NEW_TAGS = [
    "circus", "fair", "carnival", "parade", "farm",
    "rides", "live_show", "movie", "lights", "water_play",
]


@pytest.mark.parametrize("tag", NEW_TAGS)
def test_new_tag_is_in_the_allowed_taxonomy(tag):
    assert tag in ALLOWED_TAGS


@pytest.mark.parametrize("tag,title,summary", [
    ("circus",     "Circus Vazquez",            "Acrobats and aerialists under the big top."),
    ("fair",       "Arlington County Fair",     "Exhibits, a midway and live entertainment."),
    ("carnival",   "Summer Carnival",           "Midway games and funnel cake."),
    ("parade",     "Vienna Halloween Parade",   "Costumes down Maple Avenue."),
    ("farm",       "Fall Festival",             "Hayrides, a corn maze and a pumpkin patch."),
    ("rides",      "Fair Midway",               "Buy a wristband for unlimited rides."),
    ("live_show",  "Disney on Ice",             "A touring show for the whole family."),
    ("movie",      "Family Movie Night",        "An outdoor screening on the lawn."),
    ("lights",     "Bull Run Festival of Lights", "A drive-through light display."),
    ("water_play", "Splash Pad Opening Day",    "The new sprayground opens."),
])
def test_new_tag_fires_on_representative_text(tag, title, summary):
    assert tag in _tags(title, summary)


# ---------------------------------------------------------------------------
# Regressions the tightened rules are there to prevent
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("summary", [
    "Timed tickets are sold online only and do sell out.",
    "Register online in advance to reserve your spot.",
    "Tickets available online at the box office page.",
])
def test_online_logistics_text_does_not_mark_an_event_virtual(summary):
    """
    geocode.py clears coordinates for anything tagged virtual, so a stray
    "buy tickets online" once made real destinations unmappable.
    """
    assert "virtual" not in _tags("Cox Farms Fall Festival", summary, "Cox Farms")


@pytest.mark.parametrize("title,summary", [
    ("Virtual Storytime",      "Join us for an online program on Zoom."),
    ("Science Webinar",        "A live webinar for middle schoolers."),
    ("Author Talk",            "This online event is held over Zoom."),
])
def test_genuinely_virtual_events_are_still_tagged(title, summary):
    assert "virtual" in _tags(title, summary)


@pytest.mark.parametrize("title,summary", [
    ("Arlington County Fair", "Live entertainment and fair food."),
    ("Food Drive",            "Donate non-perishable food this weekend."),
    ("Food Truck Rally",      "Food trucks on the town green."),
])
def test_incidental_food_mentions_are_not_cooking(title, summary):
    assert "cooking" not in _tags(title, summary)


@pytest.mark.parametrize("title,summary", [
    ("Kids Cooking Class",   "Learn knife skills in the teaching kitchen."),
    ("Cupcake Decorating",   "Baking and cake decorating for ages 6-10."),
    ("Junior Chef Workshop", "A chef leads a hands-on session."),
])
def test_real_cooking_events_are_still_tagged(title, summary):
    assert "cooking" in _tags(title, summary)


@pytest.mark.parametrize("location", [
    "Fair Oaks Mall",
    "Fair Lakes Center",
    "Fairfax Regional Library",
])
def test_nova_place_names_containing_fair_are_not_festivals(location):
    """"Fair Oaks" is a place, not an event."""
    tags = _tags("Toddler Storytime", "A weekly storytime.", location)
    assert "festival" not in tags
    assert "fair" not in tags


@pytest.mark.parametrize("title", [
    "Loudoun County Fair",
    "State Fair of Virginia",
    "Harvest Fair",
])
def test_actual_fairs_are_tagged_as_both_fair_and_festival(title):
    tags = _tags(title, "Rides, exhibits and entertainment.")
    assert "fair" in tags
    assert "festival" in tags


# ---------------------------------------------------------------------------
# Source-declared tags
# ---------------------------------------------------------------------------

def test_extra_tags_from_the_source_are_merged_in():
    tags = set(derive_tags({
        "title": "Some Outing",
        "summary": "",
        "location_name": "",
        "start": datetime(2026, 8, 15, 14, 0),
        "extra_tags": ["circus", "all_ages"],
    }))
    assert {"circus", "all_ages"} <= tags


def test_extra_tags_outside_the_taxonomy_are_dropped():
    tags = set(derive_tags({
        "title": "Some Outing",
        "summary": "",
        "location_name": "",
        "start": datetime(2026, 8, 15, 14, 0),
        "extra_tags": ["not_a_real_tag", "circus"],
    }))
    assert "not_a_real_tag" not in tags
    assert "circus" in tags
