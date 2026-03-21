"""
Scraper registry — maps source_id strings to scraper classes.

To add a new scraper:
1. Create scrapers/<source_id>.py with a class that subclasses BaseScraper.
2. Import it here and add an entry to SCRAPERS.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from scrapers.alexandria_library import AlexandriaLibraryScraper
from scrapers.arlington_library import ArlingtonLibraryScraper
from scrapers.arlington_parks import ArlingtonParksRecScraper
from scrapers.base import BaseScraper
from scrapers.fairfax_library import FairfaxLibraryScraper
from scrapers.fairfax_parks import FairfaxParksAuthorityScraper
from scrapers.loudoun_library import LoudounLibraryScraper
from scrapers.nova_parks import NoVAParksScraper

if TYPE_CHECKING:
    pass

# Maps source_id -> scraper class (not instance)
SCRAPERS: dict[str, type[BaseScraper]] = {
    "fairfax_park_authority":  FairfaxParksAuthorityScraper,
    "arlington_parks_rec":     ArlingtonParksRecScraper,
    "fairfax_county_library":  FairfaxLibraryScraper,
    "arlington_public_library": ArlingtonLibraryScraper,
    "loudoun_county_library":  LoudounLibraryScraper,
    "alexandria_library":      AlexandriaLibraryScraper,
    "nova_parks":              NoVAParksScraper,
}


def get_scraper(source_id: str) -> BaseScraper:
    """Instantiate and return the scraper for the given source_id."""
    cls = SCRAPERS.get(source_id)
    if cls is None:
        raise ValueError(
            f"No scraper registered for source_id '{source_id}'. "
            f"Available: {list(SCRAPERS.keys())}"
        )
    return cls()
