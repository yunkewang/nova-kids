"""
Unified event schema for the NoVA Kids family activities pipeline.

All events are normalized into this Pydantic model before enrichment,
deduplication, validation, and publishing.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class CostType(str, Enum):
    FREE = "free"
    PAID = "paid"
    SLIDING_SCALE = "sliding_scale"
    SUGGESTED_DONATION = "suggested_donation"
    UNKNOWN = "unknown"


# Allowed tag values for MVP.  Additional tags may be added here as the
# taxonomy grows; the validator in Event enforces membership.
ALLOWED_TAGS: frozenset[str] = frozenset(
    {
        # Setting
        "indoor",
        "outdoor",
        "virtual",
        # Cost
        "free",
        # Timing
        "weekend",
        "weekday",
        "morning",
        "afternoon",
        "evening",
        # Age focus
        "toddler",       # 0-3
        "preschool",     # 3-5
        "elementary",    # 5-12
        "teen",          # 13+
        "all_ages",
        # Activity type
        "storytime",
        "stem",
        "arts",
        "sports",
        "nature",
        "music",
        "crafts",
        "theater",
        "cooking",
        "fitness",
        "holiday",
        "festival",
        "workshop",
        "camp",
        "swim",
        "hiking",
        # Weather
        "rainy_day",
    }
)


# ---------------------------------------------------------------------------
# Event model
# ---------------------------------------------------------------------------

class Event(BaseModel):
    """A single normalized family-friendly event."""

    # ---- Identity ----------------------------------------------------------
    id: str = Field(
        description="Stable unique identifier (hash of title+start+location+source_url)."
    )
    source_name: str = Field(description="Human-readable name of the originating source.")
    source_url: str = Field(description="Direct URL to the source event page.")

    # ---- Core content ------------------------------------------------------
    title: str = Field(description="Event title, normalized to title case.")
    summary: Optional[str] = Field(
        default=None,
        description="Short description (<280 chars). Derived from source; never fabricated.",
    )

    # ---- Timing ------------------------------------------------------------
    start: datetime = Field(description="Event start time as an aware or naive datetime.")
    end: Optional[datetime] = Field(default=None, description="Event end time if known.")
    all_day: bool = Field(default=False, description="True if the event has no specific time.")

    # ---- Location ----------------------------------------------------------
    location_name: Optional[str] = Field(default=None, description="Venue or location name.")
    location_address: Optional[str] = Field(default=None, description="Street address.")
    latitude: Optional[float] = Field(default=None, ge=-90.0, le=90.0)
    longitude: Optional[float] = Field(default=None, ge=-180.0, le=180.0)
    city: Optional[str] = Field(default=None)
    county: Optional[str] = Field(
        default=None,
        description="Virginia county or independent city (e.g. 'Fairfax', 'Arlington').",
    )

    # ---- Audience ----------------------------------------------------------
    age_min: Optional[int] = Field(default=None, ge=0, le=99)
    age_max: Optional[int] = Field(default=None, ge=0, le=99)

    # ---- Cost --------------------------------------------------------------
    cost_type: CostType = Field(default=CostType.UNKNOWN)
    price_text: Optional[str] = Field(default=None, description="Raw price string from source.")

    # ---- Tags / Scores -----------------------------------------------------
    tags: list[str] = Field(
        default_factory=list,
        description="Derived classification tags from ALLOWED_TAGS.",
    )
    family_friendly_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="0–1 score derived from tags, cost, and audience fields.",
    )
    rainy_day_friendly: bool = Field(
        default=False,
        description="True when the event is suitable regardless of weather (indoor/virtual).",
    )

    # ---- Links / Media -----------------------------------------------------
    registration_url: Optional[str] = Field(
        default=None, description="Direct registration or ticket link."
    )
    image_url: Optional[str] = Field(default=None, description="Event banner or thumbnail URL.")

    # ---- Provenance --------------------------------------------------------
    last_verified_at: datetime = Field(
        description="UTC timestamp of when the pipeline last confirmed this event exists."
    )

    # ---- Validators --------------------------------------------------------

    @field_validator("title")
    @classmethod
    def title_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("title must not be empty")
        return v.strip()

    @field_validator("tags")
    @classmethod
    def tags_in_allowed_set(cls, v: list[str]) -> list[str]:
        invalid = set(v) - ALLOWED_TAGS
        if invalid:
            raise ValueError(f"tags contain disallowed values: {invalid}")
        return sorted(set(v))  # deduplicate and sort for determinism

    @model_validator(mode="after")
    def age_range_consistent(self) -> "Event":
        if self.age_min is not None and self.age_max is not None:
            if self.age_min > self.age_max:
                raise ValueError("age_min must be <= age_max")
        return self

    @model_validator(mode="after")
    def end_after_start(self) -> "Event":
        if self.end is not None and self.end < self.start:
            raise ValueError("end must be >= start")
        return self

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
        use_enum_values = True
