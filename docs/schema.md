# Event Schema Reference

All events in the NoVA Kids pipeline are represented as instances of the
`Event` Pydantic model defined in `config/schema.py`.

---

## Field Reference

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | `str` | Yes | Stable 16-char hex ID derived from title + start + location + source_url |
| `title` | `str` | Yes | Normalized title-case event name |
| `summary` | `str \| null` | No | Short description ≤500 chars. Derived from source; never fabricated |
| `start` | `datetime` | Yes | Event start (ISO 8601) |
| `end` | `datetime \| null` | No | Event end (ISO 8601) |
| `all_day` | `bool` | No | True when no specific time is given |
| `location_name` | `str \| null` | No | Venue or park name |
| `location_address` | `str \| null` | No | Street address |
| `latitude` | `float \| null` | No | WGS-84 latitude |
| `longitude` | `float \| null` | No | WGS-84 longitude |
| `city` | `str \| null` | No | City name (inferred from location text) |
| `county` | `str \| null` | No | VA county / independent city (e.g. "Fairfax", "Arlington") |
| `age_min` | `int \| null` | No | Minimum recommended age (0–99) |
| `age_max` | `int \| null` | No | Maximum recommended age (0–99) |
| `cost_type` | `CostType` | No | Enum: `free`, `paid`, `sliding_scale`, `suggested_donation`, `unknown` |
| `price_text` | `str \| null` | No | Raw price string from source |
| `tags` | `list[str]` | No | Derived classification tags (see allowed set below) |
| `family_friendly_score` | `float` | No | 0–1 derived score |
| `rainy_day_friendly` | `bool` | No | True when suitable regardless of weather |
| `source_name` | `str` | Yes | Human-readable source name |
| `source_url` | `str` | Yes | Direct link to the source event page |
| `registration_url` | `str \| null` | No | Registration or ticketing link |
| `image_url` | `str \| null` | No | Event banner/thumbnail URL |
| `last_verified_at` | `datetime` | Yes | UTC timestamp of last pipeline confirmation |
| `extracted_from` | `str` | No | Provenance: `direct_scraper` \| `seed_resolved` \| `manual_review_approved` |
| `extraction_confidence` | `float` | No | 0–1 confidence in extracted data completeness. 1.0 for direct scrapers. |
| `short_note` | `str \| null` | No | Single derived sentence ≤200 chars. Fact-only. Never from DullesMoms. |

---

## Allowed Tags

Tags are derived automatically by the enrichment layer and must come from
this fixed set for the MVP:

### Setting
- `indoor`, `outdoor`, `virtual`

### Cost
- `free`

### Timing
- `weekend`, `weekday`, `morning`, `afternoon`, `evening`

### Age Focus
- `toddler` (0–3), `preschool` (3–5), `elementary` (5–12), `teen` (13+), `all_ages`

### Activity Type
- `storytime`, `stem`, `arts`, `crafts`, `music`, `theater`, `sports`
- `swim`, `hiking`, `nature`, `cooking`, `fitness`
- `workshop`, `camp`, `festival`, `holiday`
- `animals`, `train`, `museum`

### Weather
- `rainy_day`

To add new tags, edit `ALLOWED_TAGS` in `config/schema.py` and add
corresponding keyword rules in `enrichment/enrich.py`.

---

## Family-Friendly Score

`family_friendly_score` is a 0–1 float computed by `enrichment/enrich.py`:

| Condition | Points |
|---|---|
| Tagged all_ages, toddler, preschool, or elementary | +0.30 |
| Free event | +0.15 |
| Indoor | +0.10 |
| Enriching activity tag (storytime, arts, STEM, etc.) | +0.10 |
| Weekend timing | +0.10 |
| Rainy-day friendly | +0.05 |
| Has summary | +0.05 |
| Has image_url | +0.05 |
| Has registration_url | +0.05 |
| Teen-only (no all_ages or elementary) | −0.10 |

The score is capped at 1.0 and rounded to 4 decimal places.

---

## ID Generation

The `id` field is a 16-character lowercase hex string:

```python
sha256(
    title.lower() + "|" +
    start.isoformat() + "|" +
    (location_name or "").lower() + "|" +
    source_url
)[:16]
```

The same event scraped twice will produce the same ID, making the pipeline
idempotent and safe to re-run.

---

## Example Event Object

```json
{
  "id": "a3f8c12d9e7b0541",
  "title": "Saturday Morning Storytime",
  "summary": "Join us for stories, songs, and a simple craft. Best for ages 2-6.",
  "start": "2025-06-07T10:30:00",
  "end": "2025-06-07T11:15:00",
  "all_day": false,
  "location_name": "Tysons-Pimmit Regional Library",
  "location_address": "7584 Leesburg Pike, Falls Church, VA 22043",
  "latitude": 38.9012,
  "longitude": -77.2198,
  "city": "Falls Church",
  "county": "Fairfax",
  "age_min": 2,
  "age_max": 6,
  "cost_type": "free",
  "price_text": "Free",
  "tags": ["all_ages", "free", "indoor", "morning", "preschool", "storytime", "toddler", "weekend"],
  "family_friendly_score": 0.9,
  "rainy_day_friendly": true,
  "source_name": "Fairfax County Public Library",
  "source_url": "https://www.fairfaxcounty.gov/library/events/saturday-storytime-123",
  "registration_url": null,
  "image_url": null,
  "last_verified_at": "2025-06-03T02:00:00+00:00"
}
```
