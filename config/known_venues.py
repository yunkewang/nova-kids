"""
Known-venue lookup table for the NoVA Kids pipeline.

Maps a lowercase venue keyword (substring of location_name, title, or source_url)
to a hint dict that overrides or supplements enrichment.

Rules:
  - "tags" are MERGED (union) with derived tags.
  - "rainy_day_friendly" OVERRIDES the derived value only when the key is present.
  - "city" / "county" are set only when the event has none yet.
"""

from __future__ import annotations

from typing import Any

VenueHint = dict[str, Any]

# Key = lowercase substring matched anywhere in location_name (case-insensitive).
# Order matters: first match wins.
KNOWN_VENUES: list[tuple[str, VenueHint]] = [
    # ── Aquariums / Zoos ────────────────────────────────────────────────────
    ("national aquarium",       {"tags": ["indoor", "museum", "animals"], "rainy_day_friendly": True}),
    ("aquarium",                {"tags": ["indoor", "museum", "animals"], "rainy_day_friendly": True}),
    ("leesburg animal park",    {"tags": ["outdoor", "animals"],          "rainy_day_friendly": False, "county": "Loudoun"}),
    # ── Museums ─────────────────────────────────────────────────────────────
    ("national children's museum", {"tags": ["indoor", "museum"],             "rainy_day_friendly": True}),
    ("childrens museum",           {"tags": ["indoor", "museum"],             "rainy_day_friendly": True}),
    ("children's science center",  {"tags": ["indoor", "museum", "stem"],    "rainy_day_friendly": True, "city": "Fairfax", "county": "Fairfax"}),
    ("smithsonian",                {"tags": ["indoor", "museum"],             "rainy_day_friendly": True, "city": "Washington"}),
    ("natural history",            {"tags": ["indoor", "museum", "nature"],"rainy_day_friendly": True}),
    ("air and space",              {"tags": ["indoor", "museum", "stem"], "rainy_day_friendly": True}),
    ("udvar",                      {"tags": ["indoor", "museum", "stem"], "rainy_day_friendly": True}),
    ("airandspace",                {"tags": ["indoor", "museum", "stem"], "rainy_day_friendly": True}),
    ("tudor place",                {"tags": ["indoor", "museum"],         "rainy_day_friendly": True, "city": "Washington"}),
    # ── Malls / Indoor Destinations ──────────────────────────────────────────
    ("tysons corner",              {"tags": ["indoor"],                   "rainy_day_friendly": True, "city": "McLean", "county": "Fairfax"}),
    ("fair oaks mall",             {"tags": ["indoor"],                   "rainy_day_friendly": True, "county": "Fairfax"}),
    # ── Libraries ───────────────────────────────────────────────────────────
    ("public library",      {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("county library",      {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("branch library",      {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("cascades library",    {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Loudoun"}),
    ("loco lib",            {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Loudoun"}),
    ("barnes & noble",      {"tags": ["indoor", "storytime"], "rainy_day_friendly": True}),
    ("barnes and noble",    {"tags": ["indoor", "storytime"], "rainy_day_friendly": True}),
    ("scrawl books",        {"tags": ["indoor", "storytime"], "rainy_day_friendly": True}),
    # ── Community / Recreation Centers ──────────────────────────────────────
    ("community center",    {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("recreation center",   {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("rcc hunters woods",   {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Reston", "county": "Fairfax"}),
    ("rcc ",                {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Fairfax"}),
    ("sterling community",  {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Sterling", "county": "Loudoun"}),
    ("madison community",   {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Fairfax"}),
    ("minnie h. peyton",    {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Fairfax"}),
    # ── Ice / Skating ────────────────────────────────────────────────────────
    ("ice house",           {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    ("ice rink",            {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    ("skating rink",        {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    ("ashburn ice",         {"tags": ["indoor", "sports"], "rainy_day_friendly": True, "city": "Ashburn", "county": "Loudoun"}),
    ("bush tabernacle",     {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    # ── Indoor Play ──────────────────────────────────────────────────────────
    ("hyper kidz",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Ashburn", "county": "Loudoun"}),
    ("hyperkidz",           {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Ashburn", "county": "Loudoun"}),
    ("trampoline",          {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    # ── Arts / Studios ───────────────────────────────────────────────────────
    ("paintbar",            {"tags": ["indoor", "arts"],  "rainy_day_friendly": True}),
    ("paint studio",        {"tags": ["indoor", "arts"],  "rainy_day_friendly": True}),
    ("muse ",               {"tags": ["indoor", "arts"],  "rainy_day_friendly": True}),
    ("pottery",             {"tags": ["indoor", "arts"],  "rainy_day_friendly": True}),
    # ── Performing Arts / Theaters ───────────────────────────────────────────
    ("theater",             {"tags": ["indoor", "theater"], "rainy_day_friendly": True}),
    ("theatre",             {"tags": ["indoor", "theater"], "rainy_day_friendly": True}),
    # ── Restaurants / Dining ─────────────────────────────────────────────────
    ("restaurant",          {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("rigatoni grill",      {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("mount vernon inn",    {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Alexandria", "county": "Fairfax"}),
    # ── Nature Centers ───────────────────────────────────────────────────────
    ("nature center",       {"tags": ["indoor", "nature", "museum"], "rainy_day_friendly": True}),
    ("hidden oaks",         {"tags": ["indoor", "nature", "museum"], "rainy_day_friendly": True, "county": "Fairfax"}),
    ("potomac overlook",    {"tags": ["outdoor", "nature"],          "rainy_day_friendly": False, "county": "Arlington"}),
    # ── Farms / Outdoor ──────────────────────────────────────────────────────
    ("great country farms", {"tags": ["outdoor", "nature", "animals"], "rainy_day_friendly": False, "county": "Loudoun"}),
    ("farm",                {"tags": ["outdoor", "nature"],            "rainy_day_friendly": False}),
    ("franklin park",       {"tags": ["outdoor", "nature"],            "rainy_day_friendly": False, "county": "Loudoun"}),
    # ── Schools / Educational ────────────────────────────────────────────────
    ("montessori",          {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("lcps",                {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Loudoun"}),
    ("family academy",      {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Loudoun"}),
    # ── Sports Facilities ────────────────────────────────────────────────────
    # No "sports" tag here — venue hosts sports AND non-sports events (e.g. consignment sales)
    ("sportsplex",          {"tags": ["indoor"], "rainy_day_friendly": True, "county": "Loudoun"}),
    ("dulles sportsplex",   {"tags": ["indoor"], "rainy_day_friendly": True, "county": "Loudoun"}),
    # ── Toy Libraries / Indoor Play ────────────────────────────────────────────
    ("toy nest",            {"tags": ["indoor"], "rainy_day_friendly": True, "city": "Falls Church", "county": "Fairfax"}),
    # ── Cooking / Culinary Studios ─────────────────────────────────────────────
    ("cookology",           {"tags": ["indoor", "cooking"], "rainy_day_friendly": True}),
    # ── Preschools / Educational Centers ──────────────────────────────────────
    ("hope preschool",      {"tags": ["indoor", "preschool"], "rainy_day_friendly": True, "city": "Ashburn", "county": "Loudoun"}),
    # ── Animal Shelters / Welfare ──────────────────────────────────────────────
    ("animal welfare league", {"tags": ["indoor", "animals"], "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    # ── Performing Arts / Therapy Centers ──────────────────────────────────────
    ("a place to be",       {"tags": ["indoor", "theater"], "rainy_day_friendly": True}),
    # ── Tea / Social Experiences ───────────────────────────────────────────────
    ("tea with mrs",        {"tags": ["indoor"], "rainy_day_friendly": True}),
    # ── Shopping / Event Centers ───────────────────────────────────────────────
    ("tackett's mill",      {"tags": ["indoor"], "rainy_day_friendly": True, "county": "Prince William"}),
    # ── Chocolate / Confectionery Studios ──────────────────────────────────────
    ("conche",              {"tags": ["indoor", "cooking"], "rainy_day_friendly": True}),
    # ── Virtual ──────────────────────────────────────────────────────────────
    ("virtual",             {"tags": ["virtual"],          "rainy_day_friendly": True}),
]


def lookup_venue(location_text: str | None) -> VenueHint | None:
    """
    Return the first matching VenueHint for the given location text, or None.

    Matching is case-insensitive substring against the keys in KNOWN_VENUES.
    Also checks against source_url / title if passed as additional context.
    """
    if not location_text:
        return None
    lower = location_text.lower()
    for keyword, hint in KNOWN_VENUES:
        if keyword in lower:
            return hint
    return None


def lookup_venue_multi(*texts: str | None) -> VenueHint | None:
    """Check multiple text fields (e.g., location_name, title, source_url) in order."""
    for text in texts:
        hint = lookup_venue(text)
        if hint is not None:
            return hint
    return None
