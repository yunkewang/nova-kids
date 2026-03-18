"""
Source-name normalization for the NoVA Kids pipeline.

Maps domain names (from source_url) and raw scraped source_name strings to
human-readable display names for the iOS app.

Public entry point: normalize_source_name(source_url, raw_name) -> str | None
"""

from __future__ import annotations

from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Domain → display name  (longest/most-specific subdomains first)
# ---------------------------------------------------------------------------

_DOMAIN_MAP: dict[str, str] = {
    # Aquariums / Animals / Welfare
    "aqua.org":                      "National Aquarium",
    "leesburganimalpark.com":        "Leesburg Animal Park",
    "portdiscovery.org":             "Port Discovery Children's Museum",
    "awla.org":                      "Animal Welfare League of Arlington",
    # Museums / Science
    "nationalchildrensmuseum.org":   "National Children's Museum",
    "airandspace.si.edu":            "National Air and Space Museum",
    "si.edu":                        "Smithsonian",
    "childsci.org":                  "Children's Science Center",
    # Libraries
    "library.loudoun.gov":           "Loudoun County Public Library",
    # Government / Parks
    "loudoun.gov":                   "Loudoun County",
    "fairfaxcounty.gov":             "Fairfax County",
    "arlingtonva.us":                "Arlington County",
    "alexandriava.gov":              "City of Alexandria",
    "mcleancenter.org":              "McLean Community Center",
    "lcps.org":                      "Loudoun County Public Schools",
    # Ice / Skating
    "ashburnice.com":                "Ashburn Ice House",
    "bushtabernacle.com":            "Bush Tabernacle Skating",
    # Bookstores
    "stores.barnesandnoble.com":     "Barnes & Noble",
    "barnesandnoble.com":            "Barnes & Noble",
    "scrawlbooks.com":               "Scrawl Books",
    # Restaurants / Food
    "mountvernonrestaurant.com":     "Mount Vernon Inn Restaurant",
    "scottosrigatonigrill.com":      "Scotto's Rigatoni Grill",
    "theconchestudio.com":           "The Conche Chocolate Studio",
    "daveandbusters.com":            "Dave & Buster's",
    "cookology.com":                 "Cookology Culinary School",
    # Arts
    "musepaintbar.com":              "Muse Paintbar",
    "kidcreate.com":                 "Kidcreate Studio",
    "aplacetobeva.org":              "A Place To Be",
    # STEM / Education
    "astarexplorer.com":             "Astar Explorer",
    "hopepreschool.com":             "HOPE Preschool",
    "lomamontessori.com":            "Little Oaks Montessori Academy",
    "pepparentonline.com":           "PEP Parent Online",
    # Farms / Outdoor
    "greatcountryfarms.com":         "Great Country Farms",
    # Malls / Retail
    "tysonscornercenter.com":        "Tysons Corner Center",
    "tackettsmill.com":              "Tackett's Mill",
    "michaels.com":                  "Michaels Craft Store",
    # Sports / Recreation
    "dullessportsplex.com":          "Dulles SportsPlex",
    "hisawyer.com":                  "HiSawyer Cooking School",
    # Family / Community
    "thetoynest.com":                "The Toy Nest",
    "teawithmrsb.com":               "Tea with Mrs. B",
    "scrawlbooks.com":               "Scrawl Books",
    # Performing Arts
    "aldentheatre.org":              "Alden Theatre",
    # Children's programs
    "portdiscovery.org":             "Port Discovery Children's Museum",
}

# ---------------------------------------------------------------------------
# Raw scraped name → display name  (for when URL-based lookup fails)
# ---------------------------------------------------------------------------

_NAME_MAP: dict[str, str] = {k.lower(): v for k, v in {
    "Aqua":                      "National Aquarium",
    "Mountvernonrestaurant":     "Mount Vernon Inn Restaurant",
    "Nationalchildrensmuseum":   "National Children's Museum",
    "Stores.Barnesandnoble":     "Barnes & Noble",
    "Ashburnice":                "Ashburn Ice House",
    "Bushtabernacle":            "Bush Tabernacle Skating",
    "Lomamontessori":            "Little Oaks Montessori Academy",
    "Pepparentonline":           "PEP Parent Online",
    "Greatcountryfarms":         "Great Country Farms",
    "Leesburganimalpark":        "Leesburg Animal Park",
    "Musepaintbar":              "Muse Paintbar",
    "Tysonscornercenter":        "Tysons Corner Center",
    "Airandspace.Si":            "National Air and Space Museum",
    "Thetoynest":                "The Toy Nest",
    "Dullessportsplex":          "Dulles SportsPlex",
    "Scottosrigatonigrill":      "Scotto's Rigatoni Grill",
    "Childsci":                  "Children's Science Center",
    "Kidcreate":                 "Kidcreate Studio",
    "Hisawyer":                  "HiSawyer Cooking School",
    "Hopepreschool":             "HOPE Preschool",
    "Astarexplorer":             "Astar Explorer",
    "Library.Loudoun":           "Loudoun County Public Library",
    "Fairfaxcounty":             "Fairfax County",
    "Loudoun":                   "Loudoun County",
    "Arlingtonva":               "Arlington County",
    "Alexandriava":              "City of Alexandria",
    "Mcleancenter":              "McLean Community Center",
    "Scrawlbooks":               "Scrawl Books",
    "Teawithmrsb":               "Tea with Mrs. B",
    "Portdiscovery":             "Port Discovery Children's Museum",
    "Aplacetobeva":              "A Place To Be",
    "Daveandbusters":            "Dave & Buster's",
    "Theconchestudio":           "The Conche Chocolate Studio",
    "Tackettsmill":              "Tackett's Mill",
    "Michaels":                  "Michaels Craft Store",
    "Lcps":                      "Loudoun County Public Schools",
    "Cookology":                 "Cookology Culinary School",
    "Greatcountryfarms":         "Great Country Farms",
    "Dullessportsplex":          "Dulles SportsPlex",
    "Leesburganimalpark":        "Leesburg Animal Park",
}.items()}


def _looks_machine_generated(name: str) -> bool:
    """
    Return True if name looks like an auto-derived source name.

    Scraper-configured names are always multi-word proper phrases
    (e.g. "Fairfax County Park Authority", "Arlington County Parks & Recreation").
    Single-word names (with or without dots) are machine-generated from URLs
    (e.g. "Aqua", "Loudoun", "Ashburnice", "Stores.Barnesandnoble").
    """
    if not name:
        return True
    # Multi-word names with spaces are scraper-configured proper phrases
    return " " not in name


def normalize_source_name(
    source_url: str | None,
    raw_name: str | None,
) -> str | None:
    """
    Return a human-readable source name for display in the app.

    Priority:
      1. If raw_name is already human-readable (multiple words), keep it as-is
         so that scraper-configured names (e.g. "Fairfax County Park Authority")
         are not overwritten by a shorter domain-derived name.
      2. Domain lookup against source_url (for machine-generated names).
      3. Raw-name lookup table (fallback for machine-generated names).
      4. Return raw_name unchanged.
    """
    # 1. Human-readable name → trust it
    if raw_name and not _looks_machine_generated(raw_name):
        return raw_name

    # 2. Domain lookup (for machine-generated raw names)
    if source_url:
        try:
            host = urlparse(source_url).netloc.lower().lstrip("www.")
            if host in _DOMAIN_MAP:
                return _DOMAIN_MAP[host]
            # Try base domain ("stores.barnesandnoble.com" → "barnesandnoble.com")
            parts = host.split(".")
            if len(parts) >= 2:
                parent = ".".join(parts[-2:])
                if parent in _DOMAIN_MAP:
                    return _DOMAIN_MAP[parent]
        except Exception:
            pass

    # 3. Raw-name lookup (only for machine-generated strings)
    if raw_name:
        mapped = _NAME_MAP.get(raw_name.lower())
        if mapped:
            return mapped
        return raw_name

    return raw_name
