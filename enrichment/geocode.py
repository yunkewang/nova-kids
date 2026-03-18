"""
Geocoding service for the NoVA Kids pipeline.

Enriches events with latitude/longitude using the Photon (komoot.io) geocoding
API — an OpenStreetMap-based geocoder with no API key or strict rate limit.
Results are cached persistently in data/cache/geocode_cache.json to avoid
redundant API calls across pipeline runs.

Public entry points:
    geocode_events(events, cache=None)       → (list[Event], GeoStats)
    geocode_event_dicts(dicts, cache=None)   → (list[dict], GeoStats)
    load_cache()                             → GeoCache
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from config.schema import Event
from config.settings import CACHE_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GEO_CACHE_FILE = CACHE_DIR / "geocode_cache.json"

# Photon (komoot.io) — OSM-based geocoder, no API key, no strict rate limit.
PHOTON_URL = "https://photon.komoot.io/api/"
PHOTON_USER_AGENT = "NoVAKidsPipeline/1.0 (family activities aggregator)"
PHOTON_MIN_DELAY = 0.5  # seconds — be polite; Photon has no hard rate limit


# ---------------------------------------------------------------------------
# Address normalization for geocoding
# ---------------------------------------------------------------------------

_PHONE_RE = re.compile(r"\s*\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_BOILERPLATE_RE = re.compile(
    r"Get\s+Directions?|Store\s+Hours?|Phone\s+Number|Connect\s+With\s+Us",
    re.IGNORECASE,
)
_DUP_WORD_RE = re.compile(r"\b(\w+)\s+\1\b", re.IGNORECASE)


def _normalize_geo_query(text: str) -> str:
    """
    Normalize a raw location string into a clean geocoding query.

    - Removes duplicate adjacent words ("Farm Farm" → "Farm")
    - Removes phone numbers
    - Removes URLs
    - Truncates at boilerplate markers ("Get Directions", "Store Hours", etc.)
    - Collapses whitespace and strips trailing punctuation
    """
    if not text:
        return ""
    text = _DUP_WORD_RE.sub(r"\1", text)
    text = _PHONE_RE.sub("", text)
    text = _URL_RE.sub("", text)
    m = _BOILERPLATE_RE.search(text)
    if m:
        text = text[: m.start()]
    return re.sub(r"\s+", " ", text).strip().strip(",:;-")


def _build_geo_queries(
    location_address: str | None,
    location_name: str | None,
    city: str | None,
    county: str | None,
) -> list[str]:
    """
    Build geocoding query candidates in priority order:
      1. Full location_address (most specific)
      2. venue name + city + county + VA
      3. venue name + city + VA
      4. venue name alone (last resort)
    """
    candidates: list[str] = []

    if location_address:
        q = _normalize_geo_query(location_address)
        if q:
            candidates.append(q)

    if location_name:
        name = _normalize_geo_query(location_name)
        if name:
            if city and county:
                candidates.append(f"{name}, {city}, {county} County, VA")
            if city:
                candidates.append(f"{name}, {city}, VA")
            candidates.append(name)

    # Deduplicate while preserving priority order
    seen: set[str] = set()
    result: list[str] = []
    for q in candidates:
        if q and q not in seen:
            seen.add(q)
            result.append(q)
    return result


# ---------------------------------------------------------------------------
# Geocode result & persistent cache
# ---------------------------------------------------------------------------

@dataclass
class GeoResult:
    query: str
    latitude: Optional[float]
    longitude: Optional[float]
    confidence: Optional[float]
    resolved_at: str


class GeoCache:
    """Persistent JSON-backed geocode cache keyed by normalized query string."""

    def __init__(self, path: Path = GEO_CACHE_FILE) -> None:
        self._path = path
        self._data: dict[str, dict] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text(encoding="utf-8"))
                logger.debug(
                    "Loaded %d geocode cache entries from %s",
                    len(self._data), self._path,
                )
            except Exception as exc:
                logger.warning("Could not load geocode cache: %s", exc)

    def get(self, query: str) -> Optional[GeoResult]:
        """Return cached GeoResult for query, or None if not cached."""
        entry = self._data.get(query)
        if entry is None:
            return None
        return GeoResult(
            query=entry["query"],
            latitude=entry.get("latitude"),
            longitude=entry.get("longitude"),
            confidence=entry.get("confidence"),
            resolved_at=entry.get("resolved_at", ""),
        )

    def set(self, result: GeoResult) -> None:
        self._data[result.query] = asdict(result)
        self._dirty = True

    def save(self) -> None:
        """Flush cache to disk only if modified."""
        if not self._dirty:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._data, indent=2), encoding="utf-8"
        )
        logger.debug(
            "Saved %d geocode cache entries to %s", len(self._data), self._path
        )
        self._dirty = False

    @property
    def size(self) -> int:
        return len(self._data)


def load_cache() -> GeoCache:
    """Create and return a GeoCache loaded from disk."""
    return GeoCache()


# ---------------------------------------------------------------------------
# Photon geocoding API (komoot.io — OSM-based, no API key required)
# ---------------------------------------------------------------------------

_last_request_time: float = 0.0


def _call_photon(query: str) -> Optional[GeoResult]:
    """
    Single Photon (komoot.io) API call with polite rate limiting.
    Returns GeoResult on success, None on any failure.

    Photon response is GeoJSON: features[0].geometry.coordinates = [lon, lat]
    """
    global _last_request_time
    wait = PHOTON_MIN_DELAY - (time.monotonic() - _last_request_time)
    if wait > 0:
        time.sleep(wait)

    try:
        resp = requests.get(
            PHOTON_URL,
            params={"q": query, "limit": 1},
            headers={"User-Agent": PHOTON_USER_AGENT},
            timeout=10,
        )
        _last_request_time = time.monotonic()
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        if features:
            coords = features[0]["geometry"]["coordinates"]  # [lon, lat]
            props = features[0].get("properties", {})
            # Assign a simple confidence: 0.9 if osm_id present, else 0.5
            confidence = 0.9 if props.get("osm_id") else 0.5
            return GeoResult(
                query=query,
                latitude=round(float(coords[1]), 6),
                longitude=round(float(coords[0]), 6),
                confidence=confidence,
                resolved_at=datetime.now(tz=timezone.utc).isoformat(),
            )
    except Exception as exc:
        logger.debug("Photon geocode failed for %r: %s", query, exc)
        _last_request_time = time.monotonic()

    return None


def _resolve_with_fallbacks(
    queries: list[str],
    cache: GeoCache,
) -> tuple[Optional[GeoResult], bool]:
    """
    Try each query in priority order.
    Checks cache first; calls API if not cached.
    Caches failures so future runs skip already-failed queries.

    Returns (result_or_None, was_cache_hit).
    """
    for query in queries:
        cached = cache.get(query)
        if cached is not None:
            if cached.latitude is not None:
                return cached, True    # cache hit with valid coordinates
            continue                   # cached failure — try next query

        result = _call_photon(query)
        if result is not None:
            cache.set(result)
            return result, False       # fresh API result

        # Cache the failure to avoid retrying in future runs
        cache.set(GeoResult(
            query=query,
            latitude=None,
            longitude=None,
            confidence=None,
            resolved_at=datetime.now(tz=timezone.utc).isoformat(),
        ))

    return None, False


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class GeoStats:
    total: int = 0
    already_geocoded: int = 0
    newly_geocoded: int = 0
    cache_hits: int = 0
    failed: int = 0

    @property
    def total_with_coords(self) -> int:
        return self.already_geocoded + self.newly_geocoded

    def print_summary(self) -> None:
        pct = self.total_with_coords / self.total * 100 if self.total else 0.0
        print(f"\n  Geo enrichment:")
        print(f"    events total:       {self.total}")
        print(f"    already geocoded:   {self.already_geocoded}")
        print(f"    newly geocoded:     {self.newly_geocoded}")
        print(f"    cache hits:         {self.cache_hits}")
        print(f"    failed geocodes:    {self.failed}")
        print(f"    total with coords:  {self.total_with_coords} ({pct:.0f}% mappable)")


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def geocode_events(
    events: list[Event],
    cache: Optional[GeoCache] = None,
) -> tuple[list[Event], GeoStats]:
    """
    Enrich a list of Event objects with lat/lon coordinates.

    - Skips events that already have both latitude and longitude.
    - Tries location_address first, then venue+city fallbacks.
    - Saves the cache after processing.

    Returns (enriched_events, stats).
    """
    if cache is None:
        cache = load_cache()

    stats = GeoStats(total=len(events))
    enriched: list[Event] = []

    for event in events:
        if event.latitude is not None and event.longitude is not None:
            stats.already_geocoded += 1
            enriched.append(event)
            continue

        queries = _build_geo_queries(
            event.location_address,
            event.location_name,
            event.city,
            event.county,
        )
        if not queries:
            stats.failed += 1
            enriched.append(event)
            continue

        result, was_cache_hit = _resolve_with_fallbacks(queries, cache)

        if result is not None and result.latitude is not None:
            if was_cache_hit:
                stats.cache_hits += 1
            try:
                new_event = event.model_copy(update={
                    "latitude": result.latitude,
                    "longitude": result.longitude,
                    "geo_confidence": result.confidence,
                })
                enriched.append(new_event)
                stats.newly_geocoded += 1
                logger.debug(
                    "Geocoded '%s' → (%.5f, %.5f) via %r",
                    event.title, result.latitude, result.longitude, result.query,
                )
            except Exception as exc:
                logger.warning("Could not apply geocode to '%s': %s", event.title, exc)
                enriched.append(event)
                stats.failed += 1
        else:
            stats.failed += 1
            enriched.append(event)
            logger.debug("No geocode result for '%s'", event.title)

    cache.save()
    return enriched, stats


def geocode_event_dicts(
    event_dicts: list[dict],
    cache: Optional[GeoCache] = None,
) -> tuple[list[dict], GeoStats]:
    """
    Enrich raw event dicts (from published JSON) with lat/lon.

    Used by repair-geo mode where re-constructing Event objects is unnecessary.
    Preserves all existing fields; only adds/updates latitude and longitude.

    Returns (updated_dicts, stats).
    """
    if cache is None:
        cache = load_cache()

    stats = GeoStats(total=len(event_dicts))
    updated: list[dict] = []

    for ev in event_dicts:
        if ev.get("latitude") is not None and ev.get("longitude") is not None:
            stats.already_geocoded += 1
            updated.append(ev)
            continue

        queries = _build_geo_queries(
            ev.get("location_address"),
            ev.get("location_name"),
            ev.get("city"),
            ev.get("county"),
        )
        if not queries:
            stats.failed += 1
            updated.append(ev)
            continue

        result, was_cache_hit = _resolve_with_fallbacks(queries, cache)

        if result is not None and result.latitude is not None:
            if was_cache_hit:
                stats.cache_hits += 1
            new_ev = dict(ev)
            new_ev["latitude"] = result.latitude
            new_ev["longitude"] = result.longitude
            updated.append(new_ev)
            stats.newly_geocoded += 1
            logger.debug(
                "Geocoded '%s' → (%.5f, %.5f) via %r",
                ev.get("title", "?"), result.latitude, result.longitude, result.query,
            )
        else:
            stats.failed += 1
            updated.append(ev)

    cache.save()
    return updated, stats
