"""
Geocoding service for the NoVA Kids pipeline.

Enriches events with latitude/longitude using the Photon (komoot.io) geocoding
API — an OpenStreetMap-based geocoder with no API key or strict rate limit.
Results are cached persistently in data/cache/geocode_cache.json.

All geocode queries are constrained to the NoVA / DC metro service area via
Photon's bbox parameter. Results outside the service area are rejected and the
next fallback query is tried. Virtual events are never geocoded.

Public entry points:
    geocode_events(events, cache=None)                   → (list[Event], GeoStats)
    geocode_event_dicts(dicts, cache=None, strict=False) → (list[dict], GeoStats, list[dict])
    load_cache()                                         → GeoCache
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
# Service area — Northern Virginia / DC metro bounding box
# ---------------------------------------------------------------------------
# Covers: NoVA counties, DC, and nearby DMV metro (incl. Baltimore for venues
# like the National Aquarium and Port Discovery that are legitimately included).
#
# Format used by Photon: "lon_min,lat_min,lon_max,lat_max"
# Lat 38.3–39.6, Lon -78.6–-76.3

_LAT_MIN, _LAT_MAX = 38.3, 39.6
_LON_MIN, _LON_MAX = -78.6, -76.3

_PHOTON_BBOX = f"{_LON_MIN},{_LAT_MIN},{_LON_MAX},{_LAT_MAX}"


def _is_in_service_area(lat: float, lon: float) -> bool:
    """Return True if (lat, lon) falls within the NoVA/DC metro bounding box."""
    return _LAT_MIN <= lat <= _LAT_MAX and _LON_MIN <= lon <= _LON_MAX


# ---------------------------------------------------------------------------
# Virtual / non-physical location detection
# ---------------------------------------------------------------------------

# Location names that are exactly a virtual indicator (whole-string match)
_VIRTUAL_EXACT_RE = re.compile(
    r"^(virtual|online|webinar|remote|zoom|livestream|live\s*stream|"
    r"tbd|tba|n/?a|to\s+be\s+(announced|determined)|"
    r"your\s+preferred\s+\w[\w\s]*|anywhere|various\s+locations?)$",
    re.IGNORECASE,
)

# Location names that contain virtual indicators as substrings
_VIRTUAL_CONTAINS_RE = re.compile(
    r"\b(virtual\s+event|online\s+(event|class|program|session|meeting)|"
    r"zoom\s+meeting|live\s*stream|webinar)\b",
    re.IGNORECASE,
)

# Title-based virtual indicators — strong signals that the event is online-only.
# Conservative list: only unambiguous terms (webinar is always virtual;
# "virtual X" compound phrases are clear; zoom/webex/teams in a meeting context).
_VIRTUAL_TITLE_RE = re.compile(
    r"\bwebinar\b"
    r"|webex\b"
    r"|\blivestream\b|live\s+stream\b"
    r"|\bvirtual\s+(class|event|workshop|program|session|tour|field\s+trip|"
    r"storytime|concert|camp|experience)\b"
    r"|\b(zoom|teams|webex)\s+(call|meeting|class|session|event)\b",
    re.IGNORECASE,
)


def _is_virtual_location(
    location_name: str | None,
    tags: list | None = None,
    title: str | None = None,
) -> bool:
    """
    Return True if this event is virtual/online and should not be geocoded.

    Checks (in order):
      1. tags contain "virtual"
      2. location_name is a known virtual placeholder
      3. title contains an unambiguous virtual/webinar keyword
    """
    if tags and "virtual" in tags:
        return True
    if location_name:
        stripped = location_name.strip()
        if _VIRTUAL_EXACT_RE.match(stripped):
            return True
        if _VIRTUAL_CONTAINS_RE.search(stripped):
            return True
    if title and _VIRTUAL_TITLE_RE.search(title):
        return True
    return False


def _compute_is_mappable(
    latitude: Optional[float],
    longitude: Optional[float],
    location_name: Optional[str] = None,
    tags: Optional[list] = None,
    title: Optional[str] = None,
) -> bool:
    """
    Return True only when the event has valid coordinates inside the NoVA/DC
    service area and is not a virtual/online event.
    """
    if _is_virtual_location(location_name, tags, title):
        return False
    if latitude is None or longitude is None:
        return False
    return _is_in_service_area(latitude, longitude)


# ---------------------------------------------------------------------------
# Address normalization for geocoding queries
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
    - Removes phone numbers and URLs
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


# Mapping from normalized county name → full regional qualifier
_COUNTY_TO_REGION: dict[str, str] = {
    "fairfax":        "Fairfax County, VA",
    "arlington":      "Arlington, VA",
    "loudoun":        "Loudoun County, VA",
    "prince william": "Prince William County, VA",
    "alexandria":     "Alexandria, VA",
    "falls church":   "Falls Church, VA",
    "dc":             "Washington, DC",
}


def _build_geo_queries(
    location_address: str | None,
    location_name: str | None,
    city: str | None,
    county: str | None,
) -> list[str]:
    """
    Build geocoding query candidates in priority order:
      1. Full street address + city (most specific, avoids wrong-state matches)
      2. Full street address alone
      3. venue + city + county + VA
      4. venue + city + VA
      5. venue + county region (e.g. "Fairfax County, VA") — even without city
      6. venue alone (last resort; bbox still constrains to service area)

    Bare venue names without any regional context are always last, never first.
    The Photon bbox constraint means even bare names usually resolve correctly,
    but adding county context prevents wrong matches within the region.
    """
    candidates: list[str] = []

    addr = _normalize_geo_query(location_address or "")
    name = _normalize_geo_query(location_name or "")

    county_lower = (county or "").lower().strip()
    county_region = _COUNTY_TO_REGION.get(county_lower, "")

    # Address-based queries (highest specificity)
    if addr:
        if city:
            candidates.append(f"{addr}, {city}")
        candidates.append(addr)

    # Venue-based queries with regional context
    if name:
        if city and county:
            candidates.append(f"{name}, {city}, {county} County, VA")
        if city:
            candidates.append(f"{name}, {city}, VA")
        if county_region:
            # Include even when city is absent — critical for county-only events
            candidates.append(f"{name}, {county_region}")
        # Bare venue name — always last; bbox prevents global drift
        candidates.append(name)

    # Deduplicate preserving priority order
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

    def __init__(self, path: Path = CACHE_DIR / "geocode_cache.json") -> None:
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

    def invalidate(self, query: str) -> bool:
        """Remove a cache entry so it will be re-geocoded. Returns True if found."""
        if query in self._data:
            del self._data[query]
            self._dirty = True
            return True
        return False

    def invalidate_out_of_region(self) -> int:
        """
        Remove all cached entries whose coordinates fall outside the service area.
        Returns count of invalidated entries.
        """
        bad_keys = [
            k for k, v in self._data.items()
            if v.get("latitude") is not None
            and not _is_in_service_area(v["latitude"], v["longitude"])
        ]
        for k in bad_keys:
            del self._data[k]
        if bad_keys:
            self._dirty = True
        return len(bad_keys)

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
# Photon geocoding API — always bbox-constrained to service area
# ---------------------------------------------------------------------------

PHOTON_URL = "https://photon.komoot.io/api/"
PHOTON_USER_AGENT = "NoVAKidsPipeline/1.0 (family activities aggregator)"
PHOTON_MIN_DELAY = 0.5  # seconds — polite rate limiting

_last_request_time: float = 0.0


def _call_photon(query: str) -> Optional[GeoResult]:
    """
    Call Photon with service-area bbox constraint.
    Returns GeoResult on success, None on failure or empty results.
    """
    global _last_request_time
    wait = PHOTON_MIN_DELAY - (time.monotonic() - _last_request_time)
    if wait > 0:
        time.sleep(wait)

    try:
        resp = requests.get(
            PHOTON_URL,
            params={"q": query, "limit": 1, "bbox": _PHOTON_BBOX},
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
    Try each query in priority order, with service-area validation at every step.

    - Cache hits with out-of-region coordinates are skipped (bad old entries).
    - API results outside the service area are rejected and cached as failures.
    - Returns the first result that is within the service area.

    Returns (result_or_None, was_cache_hit).
    """
    for query in queries:
        cached = cache.get(query)
        if cached is not None:
            if cached.latitude is not None:
                if _is_in_service_area(cached.latitude, cached.longitude):
                    return cached, True   # valid cache hit
                # Cached but out of service area — skip to next query
                logger.debug(
                    "Skipping out-of-region cached result for %r (%.4f, %.4f)",
                    query, cached.latitude, cached.longitude,
                )
                continue
            continue  # cached failure

        # Not in cache — call the API (always bbox-constrained)
        result = _call_photon(query)
        if result is not None and result.latitude is not None:
            if _is_in_service_area(result.latitude, result.longitude):
                cache.set(result)
                return result, False  # fresh valid result
            # Out of region even with bbox — cache as failure, try next query
            logger.debug(
                "Rejected out-of-region API result for %r: (%.4f, %.4f)",
                query, result.latitude, result.longitude,
            )
            cache.set(GeoResult(
                query=query,
                latitude=None, longitude=None, confidence=None,
                resolved_at=datetime.now(tz=timezone.utc).isoformat(),
            ))
        else:
            # API returned nothing within bbox — cache failure
            cache.set(GeoResult(
                query=query,
                latitude=None, longitude=None, confidence=None,
                resolved_at=datetime.now(tz=timezone.utc).isoformat(),
            ))

    return None, False


# ---------------------------------------------------------------------------
# Stats and review output
# ---------------------------------------------------------------------------

@dataclass
class GeoStats:
    total: int = 0
    already_geocoded: int = 0
    newly_geocoded: int = 0
    cache_hits: int = 0
    failed: int = 0
    virtual_skipped: int = 0
    virtual_coords_cleared: int = 0  # coords removed from virtual events
    out_of_region_rejected: int = 0  # existing coords that were nulled
    retried: int = 0                 # events re-geocoded after bad coord nulled
    total_mappable: int = 0          # events with is_mappable=True after run
    total_non_mappable: int = 0      # events with is_mappable=False after run

    @property
    def total_with_coords(self) -> int:
        return self.already_geocoded + self.newly_geocoded

    def print_summary(self) -> None:
        print(f"\n  Geo enrichment:")
        print(f"    events total:              {self.total}")
        print(f"    virtual events detected:   {self.virtual_skipped}")
        if self.virtual_coords_cleared:
            print(f"    coords cleared (virtual):  {self.virtual_coords_cleared}")
        print(f"    already geocoded:          {self.already_geocoded}")
        print(f"    newly geocoded:            {self.newly_geocoded}")
        print(f"    cache hits:                {self.cache_hits}")
        if self.out_of_region_rejected:
            print(f"    out-of-region nulled:      {self.out_of_region_rejected}")
        if self.retried:
            print(f"    retried (bad coord):       {self.retried}")
        print(f"    failed geocodes:           {self.failed}")
        if self.total_mappable or self.total_non_mappable:
            print(f"    ---")
            print(f"    total mappable:            {self.total_mappable}")
            print(f"    total non-mappable:        {self.total_non_mappable}")


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def geocode_events(
    events: list[Event],
    cache: Optional[GeoCache] = None,
) -> tuple[list[Event], GeoStats]:
    """
    Enrich Event objects with lat/lon.

    - Virtual events are skipped (no coordinates assigned).
    - All geocode results are bbox-constrained to the NoVA/DC service area.
    - Events with existing out-of-region coordinates are re-geocoded.
    - Returns (enriched_events, stats).
    """
    if cache is None:
        cache = load_cache()

    stats = GeoStats(total=len(events))
    enriched: list[Event] = []

    for event in events:
        tags = list(event.tags or [])

        # Never geocode virtual events (check tags, location_name, and title)
        if _is_virtual_location(event.location_name, tags, event.title):
            if event.latitude is not None:
                stats.virtual_coords_cleared += 1
                try:
                    event = event.model_copy(update={
                        "latitude": None, "longitude": None,
                        "geo_confidence": None, "is_mappable": False,
                    })
                except Exception:
                    pass
            stats.virtual_skipped += 1
            enriched.append(event)
            continue

        # Check existing coordinates
        if event.latitude is not None and event.longitude is not None:
            if _is_in_service_area(event.latitude, event.longitude):
                stats.already_geocoded += 1
                enriched.append(event)
                continue
            # Has coordinates but outside service area — null and retry
            stats.out_of_region_rejected += 1
            stats.retried += 1
            # Fall through to re-geocode

        queries = _build_geo_queries(
            event.location_address, event.location_name, event.city, event.county
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
            # Ensure any bad existing coordinates are cleared
            if event.latitude is not None:
                try:
                    event = event.model_copy(update={
                        "latitude": None, "longitude": None, "geo_confidence": None,
                    })
                except Exception:
                    pass
            stats.failed += 1
            enriched.append(event)

    # Final pass: compute is_mappable for every event based on final coordinate state
    final_enriched: list[Event] = []
    for event in enriched:
        tags = list(event.tags or [])
        correct = _compute_is_mappable(
            event.latitude, event.longitude, event.location_name, tags, event.title
        )
        if event.is_mappable != correct:
            try:
                event = event.model_copy(update={"is_mappable": correct})
            except Exception:
                pass
        if correct:
            stats.total_mappable += 1
        else:
            stats.total_non_mappable += 1
        final_enriched.append(event)

    cache.save()
    return final_enriched, stats


def geocode_event_dicts(
    event_dicts: list[dict],
    cache: Optional[GeoCache] = None,
    strict_region: bool = False,
) -> tuple[list[dict], GeoStats, list[dict]]:
    """
    Enrich raw event dicts (from published JSON) with lat/lon.

    strict_region=True activates repair behavior:
      - Events with existing out-of-region coordinates are nulled and re-geocoded.
      - Virtual events with coordinates are nulled.
      - Rejected/suspicious geocodes are collected and returned.

    Returns (updated_dicts, stats, suspicious_geocodes).
    suspicious_geocodes is a list of dicts suitable for writing to a review file.
    """
    if cache is None:
        cache = load_cache()

    stats = GeoStats(total=len(event_dicts))
    updated: list[dict] = []
    suspicious: list[dict] = []

    for ev in event_dicts:
        tags = ev.get("tags") or []
        loc_name = ev.get("location_name")
        loc_addr = ev.get("location_address")
        title = ev.get("title", "?")
        event_id = ev.get("id", "?")
        lat = ev.get("latitude")
        lon = ev.get("longitude")

        # --- Virtual event handling ---
        if _is_virtual_location(loc_name, tags, title):
            new_ev = dict(ev)
            if lat is not None or lon is not None:
                new_ev["latitude"] = None
                new_ev["longitude"] = None
                new_ev["geo_confidence"] = None
                stats.virtual_coords_cleared += 1
                if strict_region:
                    suspicious.append({
                        "event_id": event_id,
                        "title": title,
                        "location_name": loc_name,
                        "location_address": loc_addr,
                        "original_latitude": lat,
                        "original_longitude": lon,
                        "rejection_reason": "virtual_event",
                    })
            new_ev["is_mappable"] = False
            stats.virtual_skipped += 1
            updated.append(new_ev)
            continue

        # --- Existing coordinates ---
        has_coords = lat is not None and lon is not None
        if has_coords:
            if _is_in_service_area(lat, lon):
                if strict_region:
                    # Good coords — keep as-is
                    stats.already_geocoded += 1
                    updated.append(ev)
                    continue
                else:
                    stats.already_geocoded += 1
                    updated.append(ev)
                    continue
            else:
                # Out of region
                if strict_region:
                    suspicious.append({
                        "event_id": event_id,
                        "title": title,
                        "location_name": loc_name,
                        "location_address": loc_addr,
                        "original_latitude": lat,
                        "original_longitude": lon,
                        "rejection_reason": f"out_of_service_area ({lat:.4f},{lon:.4f})",
                    })
                    stats.out_of_region_rejected += 1
                    stats.retried += 1
                    # Fall through to re-geocode
                else:
                    # Non-strict: preserve existing coordinates unchanged
                    stats.already_geocoded += 1
                    updated.append(ev)
                    continue

        # --- Geocode (new or retry) ---
        queries = _build_geo_queries(
            loc_addr, loc_name, ev.get("city"), ev.get("county")
        )
        if not queries:
            stats.failed += 1
            new_ev = dict(ev)
            new_ev["latitude"] = None
            new_ev["longitude"] = None
            updated.append(new_ev)
            continue

        result, was_cache_hit = _resolve_with_fallbacks(queries, cache)

        new_ev = dict(ev)
        if result is not None and result.latitude is not None:
            if was_cache_hit:
                stats.cache_hits += 1
            new_ev["latitude"] = result.latitude
            new_ev["longitude"] = result.longitude
            updated.append(new_ev)
            stats.newly_geocoded += 1
            logger.debug(
                "Geocoded '%s' → (%.5f, %.5f) via %r",
                title, result.latitude, result.longitude, result.query,
            )
        else:
            new_ev["latitude"] = None
            new_ev["longitude"] = None
            updated.append(new_ev)
            stats.failed += 1

    # Final pass: compute is_mappable for every event based on final coordinate state
    final_updated: list[dict] = []
    for ev in updated:
        tags = ev.get("tags") or []
        is_mappable = _compute_is_mappable(
            ev.get("latitude"), ev.get("longitude"),
            ev.get("location_name"), tags, ev.get("title"),
        )
        if ev.get("is_mappable") != is_mappable:
            ev = dict(ev)
            ev["is_mappable"] = is_mappable
        if is_mappable:
            stats.total_mappable += 1
        else:
            stats.total_non_mappable += 1
        final_updated.append(ev)

    cache.save()
    return final_updated, stats, suspicious
