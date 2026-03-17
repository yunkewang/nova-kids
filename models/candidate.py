"""
CandidateEvent model — internal pipeline use only.

A CandidateEvent represents an event *hint* discovered from a seed source
(e.g. DullesMoms calendar).  It is NOT published to the app.

Lifecycle:
  seed finder → CandidateEvent → resolver → raw dict → normalize_record → Event
                                          ↘ (low confidence) → manual_review queue
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class CandidateStatus(str, Enum):
    """Tracks where in the pipeline a candidate currently sits."""
    PENDING = "pending"          # discovered, not yet resolved
    RESOLVED = "resolved"        # original page fetched, data extracted
    PUBLISHED = "published"      # successfully normalized and published
    MANUAL_REVIEW = "manual_review"  # needs human inspection
    REJECTED = "rejected"        # explicitly discarded


class CandidateEvent(BaseModel):
    """
    An event candidate discovered from a seed/discovery source.

    This model is strictly internal — it carries provenance info and
    confidence signals used to decide whether to publish or route to review.
    It must never appear directly in published app JSON.
    """

    # ---- Identity ----------------------------------------------------------
    candidate_id: str = Field(
        description="Stable hash of seed_url + discovered_title."
    )

    # ---- Seed provenance ---------------------------------------------------
    seed_source_name: str = Field(
        description="Human-readable name of the seed source (e.g. 'DullesMoms')."
    )
    seed_url: str = Field(
        description="The seed page URL where this candidate was discovered."
    )

    # ---- Raw discovered fields (from seed page, NOT stored as content) -----
    discovered_title: str = Field(
        description="Raw title text as it appeared on the seed page."
    )
    discovered_date_text: Optional[str] = Field(
        default=None,
        description="Raw date string from seed page — used only for matching, not publishing.",
    )
    discovered_location_text: Optional[str] = Field(
        default=None,
        description="Raw location text from seed page — used only for matching.",
    )

    # ---- Original host link -----------------------------------------------
    candidate_original_url: Optional[str] = Field(
        default=None,
        description=(
            "Best outbound/original URL found on the seed page. "
            "Must NOT be a dullesmoms.com URL. "
            "None means no original link was found — requires manual review."
        ),
    )

    # ---- Resolution result ------------------------------------------------
    status: CandidateStatus = Field(default=CandidateStatus.PENDING)
    confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description=(
            "0–1 confidence that this candidate has a valid, resolvable original source. "
            "< 0.5 triggers routing to manual_review."
        ),
    )
    requires_manual_review: bool = Field(
        default=False,
        description="True when the candidate cannot be automatically resolved.",
    )
    notes: Optional[str] = Field(
        default=None,
        description="Human-readable explanation of why manual review is needed.",
    )

    # ---- Extracted facts (populated by resolver, from original page only) --
    extracted_title: Optional[str] = Field(
        default=None,
        description="Title extracted from the original host page.",
    )
    extracted_date_text: Optional[str] = Field(
        default=None,
        description="Date/time text extracted from original page.",
    )
    extracted_venue: Optional[str] = Field(
        default=None,
        description="Venue name extracted from original page.",
    )
    extracted_address: Optional[str] = Field(
        default=None,
        description="Address extracted from original page.",
    )
    extracted_cost_text: Optional[str] = Field(
        default=None,
        description="Price/cost text extracted from original page.",
    )
    extracted_description_snippet: Optional[str] = Field(
        default=None,
        description=(
            "Short factual excerpt from original page description. "
            "Max 280 chars. Used only as a source fact, not republished verbatim."
        ),
    )
    extracted_registration_url: Optional[str] = Field(
        default=None,
        description="Registration or ticket URL from original page.",
    )
    original_source_name: Optional[str] = Field(
        default=None,
        description="Name of the original host (venue, org, or domain name).",
    )

    # ---- Manual review metadata --------------------------------------------
    review_reason: Optional[str] = Field(
        default=None,
        description=(
            "Machine-readable reason this candidate is in manual review. "
            "One of: no_original_url_found, original_url_dead, "
            "extraction_incomplete, ambiguous_duplicate, low_confidence."
        ),
    )
    resolution_attempts: int = Field(
        default=0,
        description="Number of times the resolver has attempted this candidate.",
    )
    last_resolution_error: Optional[str] = Field(
        default=None,
        description="Error message from the most recent failed resolution attempt.",
    )
    suggested_next_action: Optional[str] = Field(
        default=None,
        description="Human-readable hint for what to do next in manual review.",
    )

    # ---- Timestamps --------------------------------------------------------
    discovered_at: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="UTC timestamp when this candidate was first discovered.",
    )
    resolved_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp when resolver ran against the original URL.",
    )

    # ---- Validators --------------------------------------------------------

    @model_validator(mode="after")
    def original_url_not_dullesmoms(self) -> "CandidateEvent":
        """Reject if the 'original' URL is itself a DullesMoms URL."""
        url = self.candidate_original_url or ""
        if "dullesmoms.com" in url.lower():
            # Treat as no original URL found
            self.candidate_original_url = None
            self.requires_manual_review = True
            self.notes = (
                (self.notes or "")
                + " [auto] candidate_original_url pointed to dullesmoms.com; cleared."
            ).strip()
        return self

    @model_validator(mode="after")
    def low_confidence_flags_review(self) -> "CandidateEvent":
        if self.confidence < 0.5 and not self.requires_manual_review:
            self.requires_manual_review = True
            if not self.notes:
                self.notes = f"[auto] confidence {self.confidence:.2f} < 0.5 threshold."
        return self
