"""
Base scraper class for the NoVA Kids pipeline.

Every source-specific scraper must subclass BaseScraper and implement
`fetch_raw()`.  The base class handles HTTP session setup, polite delays,
retry logic, and raw-data persistence.
"""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import (
    RAW_DIR,
    REQUEST_DELAY,
    REQUEST_MAX_RETRIES,
    REQUEST_TIMEOUT,
    USER_AGENT,
)

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    """Raised when a scraper cannot retrieve or parse source data."""


class BaseScraper(ABC):
    """
    Abstract base for all source-specific scrapers.

    Subclasses implement:
        fetch_raw() -> list[dict[str, Any]]
            Return a list of raw event dictionaries as close to the source
            data as possible.  Do NOT normalize here; that happens in the
            normalization layer.

    The base class provides:
        - A configured requests.Session with retry + User-Agent headers.
        - self.get(url) for polite HTTP GETs.
        - self.save_raw(records) to persist raw data for debugging.
    """

    #: Set by each subclass — must match an ``id`` in config/sources.yaml.
    source_id: str = ""
    source_name: str = ""

    def __init__(self) -> None:
        if not self.source_id:
            raise NotImplementedError("Subclasses must define source_id.")
        self.session = self._build_session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_raw(self) -> list[dict[str, Any]]:
        """
        Retrieve raw event data from the source.

        Returns a list of plain dicts, each representing one event.
        Keys should be as close to the source naming as possible so that
        the raw data is useful for debugging.
        """

    def run(self) -> list[dict[str, Any]]:
        """Fetch raw events, save them to disk, and return them."""
        logger.info("Running scraper: %s", self.source_id)
        try:
            records = self.fetch_raw()
        except Exception as exc:
            raise ScraperError(
                f"Scraper '{self.source_id}' failed: {exc}"
            ) from exc
        self.save_raw(records)
        logger.info(
            "Scraper '%s' fetched %d raw records.", self.source_id, len(records)
        )
        return records

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        """Polite GET with delay and timeout."""
        time.sleep(REQUEST_DELAY)
        response = self.session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
        response.raise_for_status()
        return response

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_raw(self, records: list[dict[str, Any]]) -> Path:
        """Save raw records to data/raw/<source_id>.json for debugging."""
        out_path = RAW_DIR / f"{self.source_id}.json"
        out_path.write_text(
            json.dumps(records, indent=2, default=str), encoding="utf-8"
        )
        logger.debug("Raw data saved to %s", out_path)
        return out_path

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
