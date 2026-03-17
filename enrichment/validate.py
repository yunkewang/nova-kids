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
