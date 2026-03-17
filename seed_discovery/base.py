"""
Base class for seed-discovery finders.

Seed finders are NOT scrapers.  They visit an aggregator or discovery page
to surface candidate event links.  They do not produce publishable content —
they produce CandidateEvent objects that must be resolved against original
source pages before any data is normalized or published.

Key design constraints:
  - fetch_candidates() must NOT return descriptions, summaries, or images
    from the seed source.
  - All text stored on CandidateEvent is labelled as "discovered_*" to make
    its transient, non-publishable status explicit.
  - Seeds that cannot yield a non-seed original URL are routed to manual review.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import REQUEST_DELAY, REQUEST_MAX_RETRIES, REQUEST_TIMEOUT, USER_AGENT
from models.candidate import CandidateEvent

logger = logging.getLogger(__name__)


class BaseSeedFinder(ABC):
    """
    Abstract base for all seed-discovery finders.

    Subclasses implement:
        fetch_candidates() -> list[CandidateEvent]

    The base class provides an HTTP session and polite delay helpers.
    """

    seed_source_name: str = ""  # must be set by subclass

    def __init__(self) -> None:
        if not self.seed_source_name:
            raise NotImplementedError("Subclasses must define seed_source_name.")
        self.session = self._build_session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_candidates(self) -> list[CandidateEvent]:
        """
        Visit the seed discovery page(s) and return CandidateEvent objects.

        Rules:
          - Do NOT store descriptions, summaries, or images from seed pages.
          - Only store: title, date text, location text, outbound URL.
          - Every returned candidate must have seed_source_name set.
        """

    def run(self) -> list[CandidateEvent]:
        """Run discovery and log results."""
        logger.info("Seed finder running: %s", self.seed_source_name)
        candidates = self.fetch_candidates()
        review_count = sum(1 for c in candidates if c.requires_manual_review)
        logger.info(
            "Seed finder '%s': %d candidates found, %d require manual review.",
            self.seed_source_name,
            len(candidates),
            review_count,
        )
        return candidates

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def get(self, url: str, **kwargs) -> requests.Response:
        """Polite GET with delay and timeout."""
        time.sleep(REQUEST_DELAY)
        response = self.session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
        response.raise_for_status()
        return response

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        retry = Retry(
            total=REQUEST_MAX_RETRIES,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
