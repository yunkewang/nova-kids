"""
Validation layer for the NoVA Kids pipeline.

Runs a set of deterministic checks against a list of Event objects after
normalization and deduplication, and before publishing.

Public entry point: validate_events(events: list[Event]) -> ValidationReport
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import NamedTuple

from config.schema import ALLOWED_TAGS, Event

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

class ValidationIssue(NamedTuple):
    event_id: str
    event_title: str
    rule: str
    message: str
    severity: str  # "error" | "warning"


@dataclass
class ValidationReport:
    total: int = 0
    passed: int = 0
    failed: int = 0
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    def is_clean(self) -> bool:
        """Return True only when there are zero errors (warnings allowed)."""
        return len(self.errors) == 0

    def summary(self) -> str:
        return (
            f"Validation: {self.total} events, "
            f"{self.passed} passed, {self.failed} had issues "
            f"({len(self.errors)} errors, {len(self.warnings)} warnings)"
        )


# ---------------------------------------------------------------------------
# URL validation helper
# ---------------------------------------------------------------------------

_URL_RE = re.compile(r"^https?://[^\s/$.?#].[^\s]*$", re.IGNORECASE)


def _is_valid_url(url: str | None) -> bool:
    if not url:
        return False
    return bool(_URL_RE.match(url.strip()))


# ---------------------------------------------------------------------------
# Individual rule functions
# ---------------------------------------------------------------------------

def _check_title(event: Event) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if not event.title or not event.title.strip():
        issues.append(ValidationIssue(
            event.id, event.title, "EMPTY_TITLE",
            "Title is empty or whitespace-only.", "error",
        ))
    elif len(event.title) > 300:
        issues.append(ValidationIssue(
            event.id, event.title, "TITLE_TOO_LONG",
            f"Title is {len(event.title)} chars (max 300).", "warning",
        ))
    return issues


def _check_start(event: Event) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    try:
        event.start.isoformat()  # just verify it serialises cleanly
    except Exception as exc:
        issues.append(ValidationIssue(
            event.id, event.title, "INVALID_START_DATETIME",
            f"start is not a valid datetime: {exc}", "error",
        ))
    return issues


def _check_urls(event: Event) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if not _is_valid_url(event.source_url):
        issues.append(ValidationIssue(
            event.id, event.title, "INVALID_SOURCE_URL",
            f"source_url is not a valid URL: {event.source_url!r}", "error",
        ))
    for field_name in ("registration_url", "image_url"):
        val = getattr(event, field_name, None)
        if val and not _is_valid_url(val):
            issues.append(ValidationIssue(
                event.id, event.title, f"INVALID_{field_name.upper()}",
                f"{field_name} is not a valid URL: {val!r}", "warning",
            ))
    return issues


def _check_tags(event: Event) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    invalid = set(event.tags) - ALLOWED_TAGS
    if invalid:
        issues.append(ValidationIssue(
            event.id, event.title, "DISALLOWED_TAGS",
            f"Tags not in allowed set: {sorted(invalid)}", "error",
        ))
    return issues


def _check_score_range(event: Event) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if not (0.0 <= event.family_friendly_score <= 1.0):
        issues.append(ValidationIssue(
            event.id, event.title, "SCORE_OUT_OF_RANGE",
            f"family_friendly_score={event.family_friendly_score} not in [0,1]", "error",
        ))
    return issues


def _check_summary_length(event: Event) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if event.summary and len(event.summary) > 500:
        issues.append(ValidationIssue(
            event.id, event.title, "SUMMARY_TOO_LONG",
            f"summary is {len(event.summary)} chars (max 500).", "warning",
        ))
    return issues


def _check_age_range(event: Event) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if event.age_min is not None and event.age_max is not None:
        if event.age_min > event.age_max:
            issues.append(ValidationIssue(
                event.id, event.title, "INVALID_AGE_RANGE",
                f"age_min ({event.age_min}) > age_max ({event.age_max})", "error",
            ))
    return issues


def _check_no_dullesmoms_source_url(event: Event) -> list[ValidationIssue]:
    """
    Error when source_url points to dullesmoms.com.

    DullesMoms is a discovery layer only.  Published events must have original
    host URLs.  If a DullesMoms URL appears here it means the resolver failed
    to find an original source and the event was not properly routed to review.
    """
    issues: list[ValidationIssue] = []
    if event.source_url and "dullesmoms.com" in event.source_url.lower():
        issues.append(ValidationIssue(
            event.id, event.title, "DULLESMOMS_SOURCE_URL",
            (
                "source_url points to dullesmoms.com. Published events must use "
                "the original event host URL. Route to manual review instead."
            ),
            "error",
        ))
    return issues


def _check_no_dullesmoms_registration_url(event: Event) -> list[ValidationIssue]:
    """Warn when registration_url points to dullesmoms.com."""
    issues: list[ValidationIssue] = []
    reg_url = event.registration_url or ""
    if "dullesmoms.com" in reg_url.lower():
        issues.append(ValidationIssue(
            event.id, event.title, "DULLESMOMS_REGISTRATION_URL",
            "registration_url points to dullesmoms.com. Prefer original registration link.",
            "warning",
        ))
    return issues


def _check_actionable_url(event: Event) -> list[ValidationIssue]:
    """
    Every published event must have at minimum: title + start + one valid URL.

    The URL can be source_url or registration_url.
    """
    issues: list[ValidationIssue] = []
    has_url = _is_valid_url(event.source_url) or _is_valid_url(event.registration_url)
    if not has_url:
        issues.append(ValidationIssue(
            event.id, event.title, "NO_ACTIONABLE_URL",
            "Event has no valid source_url or registration_url. App cannot link to it.",
            "error",
        ))
    return issues


def _check_short_note(event: Event) -> list[ValidationIssue]:
    """Validate short_note length and single-sentence constraint."""
    issues: list[ValidationIssue] = []
    if event.short_note is None:
        return issues
    from enrichment.annotate import validate_short_note
    is_valid, reason = validate_short_note(event.short_note)
    if not is_valid:
        issues.append(ValidationIssue(
            event.id, event.title, "INVALID_SHORT_NOTE",
            reason,
            "warning",
        ))
    return issues


def _check_shortener_url(event: Event) -> list[ValidationIssue]:
    """Warn when source_url is still a URL-shortener link (redirect not resolved)."""
    issues: list[ValidationIssue] = []
    if not event.source_url:
        return issues
    try:
        from urllib.parse import urlparse
        host = urlparse(event.source_url).netloc.lower().lstrip("www.")
        _SHORTENER_DOMAINS = frozenset([
            "bit.ly", "tinyurl.com", "t.co", "ow.ly", "buff.ly",
            "goo.gl", "short.io", "rb.gy", "cutt.ly", "is.gd",
            "lnkd.in", "dlvr.it", "rebrand.ly", "rebrandly.com",
        ])
        if host in _SHORTENER_DOMAINS:
            issues.append(ValidationIssue(
                event.id, event.title, "SHORTENER_SOURCE_URL",
                f"source_url uses a shortener domain ({host}). Redirect should have been resolved.",
                "warning",
            ))
    except Exception:
        pass
    return issues


def _check_price_text_quality(event: Event) -> list[ValidationIssue]:
    """Warn when price_text is suspiciously long (likely a scraped paragraph)."""
    issues: list[ValidationIssue] = []
    if event.price_text and len(event.price_text) > 80:
        issues.append(ValidationIssue(
            event.id, event.title, "PRICE_TEXT_TOO_LONG",
            f"price_text is {len(event.price_text)} chars (max 80). May be a noisy extraction.",
            "warning",
        ))
    return issues


def _check_enrichment_consistency(event: Event) -> list[ValidationIssue]:
    """Warn on contradictory tag/flag combinations."""
    issues: list[ValidationIssue] = []
    tag_set = set(event.tags)
    if "indoor" in tag_set and not event.rainy_day_friendly:
        issues.append(ValidationIssue(
            event.id, event.title, "INDOOR_NOT_RAINY_DAY",
            "Event is tagged 'indoor' but rainy_day_friendly=False. Check venue override.",
            "warning",
        ))
    if "outdoor" in tag_set and "indoor" not in tag_set and event.rainy_day_friendly:
        issues.append(ValidationIssue(
            event.id, event.title, "OUTDOOR_RAINY_DAY_FRIENDLY",
            "Event is tagged 'outdoor' (no 'indoor') but rainy_day_friendly=True.",
            "warning",
        ))
    return issues


def _check_seed_resolved_confidence(event: Event) -> list[ValidationIssue]:
    """Warn when a seed-resolved event has low extraction confidence."""
    issues: list[ValidationIssue] = []
    if event.extracted_from == "seed_resolved" and event.extraction_confidence < 0.6:
        issues.append(ValidationIssue(
            event.id, event.title, "LOW_EXTRACTION_CONFIDENCE",
            (
                f"seed_resolved event has extraction_confidence="
                f"{event.extraction_confidence:.2f} (< 0.6). Review for accuracy."
            ),
            "warning",
        ))
    return issues


def _check_duplicate_ids(events: list[Event]) -> list[ValidationIssue]:
    """Cross-event rule: detect duplicate IDs in the batch."""
    seen: dict[str, str] = {}
    issues: list[ValidationIssue] = []
    for event in events:
        if event.id in seen:
            issues.append(ValidationIssue(
                event.id, event.title, "DUPLICATE_ID",
                f"Duplicate id={event.id} also on event '{seen[event.id]}'", "error",
            ))
        else:
            seen[event.id] = event.title
    return issues


# ---------------------------------------------------------------------------
# Per-event rule suite
# ---------------------------------------------------------------------------

_PER_EVENT_RULES = [
    _check_title,
    _check_start,
    _check_urls,
    _check_tags,
    _check_score_range,
    _check_summary_length,
    _check_age_range,
    # Source provenance rules
    _check_no_dullesmoms_source_url,
    _check_no_dullesmoms_registration_url,
    _check_actionable_url,
    _check_short_note,
    _check_seed_resolved_confidence,
    _check_shortener_url,
    _check_price_text_quality,
    _check_enrichment_consistency,
]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def validate_events(events: list[Event]) -> ValidationReport:
    """
    Run all validation rules against the event list.

    Returns a ValidationReport.  Call .is_clean() to check for errors.
    """
    report = ValidationReport(total=len(events))
    all_issues: list[ValidationIssue] = []

    # Per-event rules
    for event in events:
        event_issues: list[ValidationIssue] = []
        for rule_fn in _PER_EVENT_RULES:
            event_issues.extend(rule_fn(event))

        if event_issues:
            report.failed += 1
            all_issues.extend(event_issues)
        else:
            report.passed += 1

    # Cross-event rules
    all_issues.extend(_check_duplicate_ids(events))

    report.issues = all_issues

    for issue in report.errors:
        logger.error("[%s] %s — %s: %s", issue.event_id, issue.event_title,
                     issue.rule, issue.message)
    for issue in report.warnings:
        logger.warning("[%s] %s — %s: %s", issue.event_id, issue.event_title,
                       issue.rule, issue.message)

    logger.info(report.summary())
    return report
