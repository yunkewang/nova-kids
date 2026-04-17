"""
Pricing classification for events.

Given the available context (raw price text, summary, title, source name,
source URL, venue name), decide how to classify an event's pricing without
defaulting to "free" just because the scraper didn't find a dollar sign.

Design goals (see docs/fix_event_pricing.md):
  - Bias toward PAID when explicit fee text exists (even from libraries / parks).
  - Bias toward UNKNOWN rather than FREE when evidence is missing.
  - Only mark FREE when the source text says so, or when the source is a public
    library / parks / community venue AND no paid-sounding text is present.
  - Preserve rich pricing details: mixed / members-only / registration-required
    events must not silently collapse into "free".

Public API:
    classify_pricing(...) -> PricingClassification
    infer_cost(...)       -> legacy tuple wrapper (kept for callers)

Every classification carries a ``reason`` and ``matched_patterns`` so the
caller can log exactly why an event was bucketed the way it was.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from config.schema import CostType, PriceType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Regex patterns — ordered from strongest to weakest signals
# ---------------------------------------------------------------------------

# Explicit dollar amount: $8, $10.00, $10-15, $10/child, $ 10
_DOLLAR_RE = re.compile(r"\$\s*\d")

# Strong paid phrases — these should override library/parks "free by default"
_STRONG_PAID_RE = re.compile(
    r"\bregistration\s+fee\b"
    r"|\bclass\s+fee\b"
    r"|\bcourse\s+fee\b"
    r"|\bprogram\s+fee\b"
    r"|\bmaterials?\s+fee\b"
    r"|\bentry\s+fee\b"
    r"|\badmission\s+fee\b"
    r"|\bticket(?:ed|\s+required)\b"
    r"|\bpurchase\s+tickets?\b"
    r"|\btickets?\s+\$"
    r"|\bper\s+child\b"
    r"|\bper\s+person\b"
    r"|\bper\s+family\b"
    r"|\bper\s+adult\b"
    r"|\bper\s+participant\b"
    r"|\bpaid\s+(?:event|admission|program)\b"
    r"|\bbook\s+now\s+for\s*\$"
    r"|\breserve\s+your\s+spot\s+for\s*\$"
    r"|\bprices?\s+start\b"
    r"|\bprice:?\s*\$"
    r"|\bcost:?\s*\$"
    r"|\bfee:?\s*\$"
    r"|\badmission:?\s*\$"
    r"|\b\d+\s*(?:usd|dollars?)\b"
    # "includes admission" = you pay for something which includes admission
    r"|\bincludes\s+admission\b"
    # parenthetical price tail, e.g. "(fee)" "(paid)" "(ticketed)"
    r"|\(\s*(?:fee|paid|ticketed|registration\s+required)\s*\)",
    re.IGNORECASE,
)

# Mixed pricing — free for some attendees, paid for others
_MIXED_PRICING_RE = re.compile(
    r"\b(?:members?|memberships?)\s+free\b"
    r"|\bfree\s+for\s+members?\b"
    r"|\bmembers?\s+(?:only)?\s*free\s*[,;/]?\s*(?:non[- ]?members?|guests?)\s*\$"
    r"|\bfree\s+with\s+(?:membership|admission|paid\s+admission|park\s+pass)\b"
    r"|\bfree\s+with\s+\$?\d+\s+(?:purchase|minimum)\b"
    # "Members free / Non-members $10"  or  "Members: free, Non-members: $10"
    r"|members?[^.]{0,40}non[- ]?members?"
    r"|\bnon[- ]?members?\s*[:\-]?\s*\$\d"
    r"|\bincluded\s+with\s+admission\b"
    r"|\bfree\s+with\s+paid\s+admission\b",
    re.IGNORECASE,
)

# Suggested-donation / pay-what-you-can language
_DONATION_RE = re.compile(
    r"\bsuggested\s+donation\b"
    r"|\bdonation[- ]based\b"
    r"|\bpay\s+what\s+you\s+can\b"
    r"|\bpay\s+what\s+you\s+wish\b"
    r"|\bfree\s+(?:with|and)\s+optional\s+donation\b"
    r"|\bvoluntary\s+donation\b"
    r"|\bsliding[- ]scale\s+fee\b"
    r"|\bdonations?\s+(?:accepted|welcome|appreciated)\b",
    re.IGNORECASE,
)

# Explicit free language
_FREE_EXPLICIT_RE = re.compile(
    r"\bfree\s+event\b"
    r"|\bfree\s+admission\b"
    r"|\bfree\s+to\s+(?:attend|the\s+public|all)\b"
    r"|\bfree\s+of\s+charge\b"
    r"|\bfree\s+for\s+(?:all|everyone|the\s+public)\b"
    r"|\bno\s+charge\b"
    r"|\bno\s+cost\b"
    r"|\bno\s+fee\b"
    r"|\bat\s+no\s+cost\b"
    r"|\bcomplimentary\b"
    r"|\badmission\s+is\s+free\b"
    r"|\bcost:\s*(?:free|\$?0(?:\.0+)?)\b"
    r"|\bfee:\s*(?:free|\$?0(?:\.0+)?)\b"
    r"|\bprice:\s*(?:free|\$?0(?:\.0+)?)\b"
    # Structured "Free" tokens commonly found in cost fields (JSON-LD)
    r"|^\s*free\s*$"
    r"|^\s*\$?0(?:\.00?)?\s*$",
    re.IGNORECASE,
)

# Weaker free signal — standalone "free" word. Applied only when no paid
# signals were found, to avoid mis-classifying "free for members, $10 otherwise".
_FREE_WORD_RE = re.compile(r"\bfree\b", re.IGNORECASE)

# Weak paid signals — apply even to public/library sources so that a library
# class with "fee" or "tickets" in the summary is marked paid, not free.
_WEAK_PAID_RE = re.compile(
    r"\btickets?\b"
    r"|\badmission(?:\s+charge)?\b"
    r"|\bfees?\b"
    r"|\bcost[s:]?\b"
    r"|\bprice[d:]?\b"
    r"|\bpaid\b",
    re.IGNORECASE,
)

# Language that falsely triggers weak-paid patterns ("no fee", "free admission")
_PAID_FALSE_POSITIVES_RE = re.compile(
    r"\bno\s+(?:fee|cost|charge|tickets?|admission|price)\b"
    r"|\bfree\s+(?:admission|tickets?|entry|fee|cost)\b"
    r"|\bfee[- ]?free\b"
    r"|\bticket[- ]?free\b",
    re.IGNORECASE,
)

# Registration-required (independent of pricing)
_REGISTRATION_REQUIRED_RE = re.compile(
    r"\bregistration\s+(?:is\s+)?required\b"
    r"|\badvance\s+registration\s+(?:is\s+)?required\b"
    r"|\bregister\s+(?:in\s+advance|online|today|now|here|below|above)\b"
    r"|\bpre[- ]?registration\b"
    r"|\brsvp\s+required\b"
    r"|\bmust\s+(?:register|rsvp|book|reserve)\b"
    r"|\badvance\s+(?:booking|reservation)\s+required\b"
    r"|\bticket\s+required\b",
    re.IGNORECASE,
)

# Patterns used to extract a short pricing_summary snippet
_PRICING_SUMMARY_RES: list[re.Pattern[str]] = [
    re.compile(
        r"(?:registration|class|course|program|entry|admission|materials?|ticket)\s+"
        r"fee[^.\n]{0,80}",
        re.IGNORECASE,
    ),
    re.compile(r"(?:cost|price|fee|admission|tickets?)\s*[:\-]\s*[^.\n]{0,80}", re.IGNORECASE),
    re.compile(r"\$\s*\d[\d,.]*(?:\s*(?:[-/]|to)\s*\$?\d[\d,.]*)?"
               r"(?:\s*(?:per\s+\w+|/\w+|each))?", re.IGNORECASE),
    re.compile(r"members?\s+free[^.\n]{0,60}", re.IGNORECASE),
    re.compile(r"free\s+for[^.\n]{0,60}", re.IGNORECASE),
    re.compile(r"suggested\s+donation[^.\n]{0,60}", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# Source-type detectors (public library / parks / community)
# ---------------------------------------------------------------------------

_LIBRARY_SOURCE_SUBSTRINGS: frozenset[str] = frozenset({
    "public library",
    "public libraries",
    "libcal",
    "libnet",
})

_PUBLIC_SOURCE_SUBSTRINGS: frozenset[str] = frozenset({
    "park authority",
    "parks & recreation",
    "parks and recreation",
    "parks recreation",
    "county parks",
    "city parks",
    "community center",
    "family resource center",
    "nature center",
    "recreation center",
    "school district",
    "public school",
})

# Title/summary keywords that almost always indicate a paid program even at
# parks, libraries, and rec centers. When one of these appears and we have no
# other pricing signal, we fall back to UNKNOWN instead of the public-source
# default of FREE — so e.g. a Fairfax Parks "Pottery Workshop" stops silently
# appearing as free just because the detail-page price didn't extract.
_PAID_PROGRAM_KEYWORDS_RE = re.compile(
    r"\b("
    r"workshop|workshops|"
    r"class|classes|"
    r"camp|camps|"
    r"course|courses|"
    r"lesson|lessons|"
    r"series|"
    r"academy|"
    r"clinic|clinics|"
    r"training|"
    r"certification|"
    r"instruction|instructor|"
    r"intensive|"
    r"seminar|seminars|"
    r"painting|drawing|sketching|pottery|ceramics|knitting|sewing|crochet|"
    r"yoga|pilates|cooking\s+class|baking\s+class|"
    r"swim(?:ming)?\s+(?:lesson|class|clinic)|"
    r"tennis\s+(?:lesson|clinic)|golf\s+(?:lesson|clinic)|"
    r"riding\s+(?:lesson|clinic)|horse\s+riding|"
    r"birthday\s+party|paint\s+night|paint\s+and\s+sip"
    r")\b",
    re.IGNORECASE,
)


def _source_is_library(source_name: str | None, source_url: str | None) -> bool:
    if source_name:
        lower = source_name.lower()
        if "library" in lower or "libraries" in lower:
            return True
        for sub in _LIBRARY_SOURCE_SUBSTRINGS:
            if sub in lower:
                return True
    if source_url:
        url_lower = source_url.lower()
        if "libcal" in url_lower or "libnet" in url_lower or "library" in url_lower:
            return True
    return False


def _source_is_public_community(source_name: str | None) -> bool:
    if not source_name:
        return False
    lower = source_name.lower()
    return any(sub in lower for sub in _PUBLIC_SOURCE_SUBSTRINGS)


def _venue_is_library(location_name: str | None) -> bool:
    if not location_name:
        return False
    lower = location_name.lower()
    return "library" in lower or "libraries" in lower


def _venue_is_public_community(location_name: str | None) -> bool:
    if not location_name:
        return False
    lower = location_name.lower()
    keywords = (
        "recreation center", "rec center", "community center",
        "nature center", "civic center", "family resource center",
        "community room",
    )
    return any(kw in lower for kw in keywords)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class PricingClassification:
    """Structured output of classify_pricing()."""

    price_type: PriceType
    cost_type: CostType
    is_free: Optional[bool]
    pricing_summary: Optional[str] = None
    price_text: Optional[str] = None
    registration_required: bool = False
    registration_fee_text: Optional[str] = None
    extracted_price_text: Optional[str] = None
    reason: str = "unknown_no_signals"
    matched_patterns: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _first_snippet(text: str, patterns: list[re.Pattern[str]]) -> Optional[str]:
    for pat in patterns:
        m = pat.search(text)
        if m:
            snippet = m.group(0).strip().strip(",.;:")
            snippet = re.sub(r"\s+", " ", snippet)
            return snippet[:200] or None
    return None


def _clean_price_text(text: Optional[str]) -> Optional[str]:
    """Strip whitespace and cap at 200 chars; return None for empty strings."""
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    return text[:200]


def _has_weak_paid_signal(text: str) -> bool:
    """True if generic paid-ish word ('fee', 'ticket', 'admission') appears.

    Returns False when the only match is inside a known false-positive phrase
    like "no fee" or "free admission".
    """
    if not _WEAK_PAID_RE.search(text):
        return False
    # Blank out false-positive phrases and re-check
    scrubbed = _PAID_FALSE_POSITIVES_RE.sub(" ", text)
    return bool(_WEAK_PAID_RE.search(scrubbed))


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def classify_pricing(
    price_text: str | None = None,
    summary: str | None = None,
    title: str | None = None,
    source_name: str | None = None,
    source_url: str | None = None,
    location_name: str | None = None,
    registration_url: str | None = None,
) -> PricingClassification:
    """
    Classify an event's pricing.

    Priority order (first match wins):
        1. Mixed pricing              → MIXED (paid, with details)
        2. Suggested donation          → DONATION (checked before $ so
                                         "Suggested donation: $5" is not PAID)
        3. Strong paid signal          → PAID
        4. Dollar amount               → PAID
        5. Explicit free text          → FREE (unless conflicting paid signal)
        6. Weak paid signal            → PAID (even for library/parks sources)
        7. Library / parks / community → FREE (source default)
        8. Standalone 'free' word      → FREE
        9. Fallback                    → UNKNOWN   (never FREE)

    Unlike the previous logic, weak paid signals (step 6) are checked BEFORE
    the library / parks "default free" fallback. That means a library page
    that says "registration fee applies" (without a dollar sign in its price
    field) is now correctly classified as PAID instead of silently FREE.
    """
    # Structured zero-cost signals in the explicit price_text field → FREE.
    # Catches "$0", "$0.00", "0", "0.00", "free" used as a value by JSON-LD
    # Offers blocks and similar.
    if price_text is not None:
        pt_clean = str(price_text).strip().lower()
        if pt_clean in {"free", "0", "0.0", "0.00", "$0", "$0.0", "$0.00"}:
            return PricingClassification(
                price_type=PriceType.FREE,
                cost_type=CostType.FREE,
                is_free=True,
                pricing_summary="Free",
                price_text="Free",
                extracted_price_text=str(price_text).strip(),
                reason="zero_cost_value",
                matched_patterns=[f"zero_value:{price_text!r}"],
            )

    raw_combined_parts = [p for p in (price_text, summary, title) if p]
    combined = " ".join(raw_combined_parts).strip()
    combined_lower = combined.lower()

    extracted = _clean_price_text(price_text) or _clean_price_text(
        _first_snippet(combined, _PRICING_SUMMARY_RES) if combined else None
    )

    matched: list[str] = []
    # Explicit wording ("must register", "advance registration required", etc.)
    # or the presence of a registration_url alongside any content. A matched
    # "registration fee ..." snippet also implies registration is required.
    registration_required = bool(
        _REGISTRATION_REQUIRED_RE.search(combined)
        or re.search(r"\bregistration\s+fee\b", combined, re.IGNORECASE)
        or (registration_url and (price_text or summary or title))
    )

    def _finalize(
        price_type: PriceType,
        cost_type: CostType,
        is_free: Optional[bool],
        reason: str,
        *,
        summary_snippet: Optional[str] = None,
        reg_fee_text: Optional[str] = None,
    ) -> PricingClassification:
        pricing_summary = summary_snippet or _first_snippet(combined, _PRICING_SUMMARY_RES) \
            if combined else None
        pricing_summary = _clean_price_text(pricing_summary) or extracted
        # Short, clean price_text preserved for legacy callers
        clean_price = _clean_price_text(price_text) or (
            pricing_summary if cost_type in (CostType.PAID, CostType.SUGGESTED_DONATION) else None
        )
        result = PricingClassification(
            price_type=price_type,
            cost_type=cost_type,
            is_free=is_free,
            pricing_summary=pricing_summary,
            price_text=clean_price,
            registration_required=registration_required,
            registration_fee_text=reg_fee_text,
            extracted_price_text=extracted,
            reason=reason,
            matched_patterns=matched,
        )
        logger.debug(
            "Pricing classified: type=%s is_free=%s reason=%s matched=%s summary=%r",
            result.price_type.value, result.is_free, result.reason,
            result.matched_patterns, result.pricing_summary,
        )
        return result

    # Empty input → UNKNOWN (not free). If we have a known public venue and
    # absolutely nothing else to go on, step 7 below handles the default.
    if not combined:
        if _source_is_library(source_name, source_url) or _venue_is_library(location_name):
            return _finalize(
                PriceType.FREE, CostType.FREE, True,
                reason="library_default_free_no_context",
            )
        if _source_is_public_community(source_name) or _venue_is_public_community(location_name):
            return _finalize(
                PriceType.FREE, CostType.FREE, True,
                reason="public_source_default_free_no_context",
            )
        return _finalize(PriceType.UNKNOWN, CostType.UNKNOWN, None, reason="no_context")

    # --- Step 1: mixed pricing ------------------------------------------------
    mixed_match = _MIXED_PRICING_RE.search(combined)
    if mixed_match:
        matched.append(f"mixed:{mixed_match.group(0)!r}")
        reg_fee = None
        if "registration" in combined_lower and _DOLLAR_RE.search(combined):
            reg_fee = _first_snippet(combined, [_PRICING_SUMMARY_RES[0]])
        return _finalize(
            PriceType.MIXED, CostType.PAID, False,
            reason="mixed_pricing",
            reg_fee_text=reg_fee,
        )

    # --- Step 2: donation ----------------------------------------------------
    # Checked BEFORE dollar / strong-paid so "Suggested donation: $5" is tagged
    # as DONATION rather than PAID.
    donation_match = _DONATION_RE.search(combined)
    if donation_match:
        matched.append(f"donation:{donation_match.group(0)!r}")
        return _finalize(
            PriceType.DONATION, CostType.SUGGESTED_DONATION, False,
            reason="donation_language",
        )

    # --- Step 3: strong paid signal ------------------------------------------
    strong_match = _STRONG_PAID_RE.search(combined)
    if strong_match:
        matched.append(f"strong_paid:{strong_match.group(0)!r}")
        reg_fee = None
        if "registration" in strong_match.group(0).lower():
            reg_fee = _first_snippet(combined, [_PRICING_SUMMARY_RES[0]])
        return _finalize(
            PriceType.PAID, CostType.PAID, False,
            reason="strong_paid_signal",
            reg_fee_text=reg_fee,
        )

    # --- Step 4: dollar amount -----------------------------------------------
    dollar_match = _DOLLAR_RE.search(combined)
    if dollar_match:
        matched.append(f"dollar_amount:{dollar_match.group(0)!r}")
        return _finalize(
            PriceType.PAID, CostType.PAID, False,
            reason="dollar_amount",
        )

    # --- Step 5: explicit free text ------------------------------------------
    explicit_free_match = _FREE_EXPLICIT_RE.search(combined)
    if explicit_free_match:
        matched.append(f"explicit_free:{explicit_free_match.group(0)!r}")
        # If the same text ALSO says 'fee applies' / 'tickets required' and so on,
        # classify as mixed rather than silently free.
        if _has_weak_paid_signal(combined):
            matched.append("conflicting_weak_paid")
            return _finalize(
                PriceType.MIXED, CostType.PAID, False,
                reason="conflicting_free_and_paid",
            )
        return _finalize(
            PriceType.FREE, CostType.FREE, True,
            reason="explicit_free_text",
        )

    # --- Step 6: weak paid signal (applies even to libraries/parks) ----------
    # This is the critical fix: previously, "fee" / "tickets" / "admission"
    # were skipped for public-source events, which caused paid library or
    # parks programs to be mis-flagged as free.
    if _has_weak_paid_signal(combined):
        weak_match = _WEAK_PAID_RE.search(combined)
        matched.append(f"weak_paid:{weak_match.group(0)!r}" if weak_match else "weak_paid")
        return _finalize(
            PriceType.PAID, CostType.PAID, False,
            reason="weak_paid_signal",
        )

    # --- Step 7: public / library / community defaults ----------------------
    # Tightened: even at libraries / parks / rec centers, if the title looks
    # like a paid-program format (workshop, class, camp, lesson, ...) and we
    # have NO explicit free signal, fall back to UNKNOWN. Silently flagging
    # a Fairfax Parks "Pottery Workshop" as free would be the same bug we're
    # trying to fix — better to surface uncertainty than to mislead users.
    is_library = _source_is_library(source_name, source_url) or _venue_is_library(location_name)
    is_public = _source_is_public_community(source_name) or _venue_is_public_community(location_name)
    if is_library or is_public:
        paid_program_match = _PAID_PROGRAM_KEYWORDS_RE.search(combined)
        if paid_program_match:
            matched.append(f"paid_program_keyword:{paid_program_match.group(0)!r}")
            return _finalize(
                PriceType.UNKNOWN, CostType.UNKNOWN, None,
                reason="paid_program_format_no_price",
            )
        if is_library:
            matched.append("library_default")
            return _finalize(
                PriceType.FREE, CostType.FREE, True,
                reason="library_default_free",
            )
        matched.append("public_default")
        return _finalize(
            PriceType.FREE, CostType.FREE, True,
            reason="public_source_default_free",
        )

    # --- Step 8: standalone 'free' word (weaker signal) ----------------------
    if _FREE_WORD_RE.search(combined):
        matched.append("free_word")
        return _finalize(
            PriceType.FREE, CostType.FREE, True,
            reason="free_word_only",
        )

    # --- Step 9: fallback → UNKNOWN (NOT free) -------------------------------
    return _finalize(
        PriceType.UNKNOWN, CostType.UNKNOWN, None,
        reason="unknown_no_signals",
    )


# ---------------------------------------------------------------------------
# Legacy tuple-returning wrapper — kept for existing callers
# ---------------------------------------------------------------------------

def infer_cost(
    price_text: str | None,
    summary: str | None = None,
    source_name: str | None = None,
    source_url: str | None = None,
    location_name: str | None = None,
    title: str | None = None,
) -> tuple[CostType, str | None, str]:
    """
    Backward-compatible shim returning (cost_type, price_text, reason).

    New code should call classify_pricing() directly to get the full structured
    classification including mixed/donation/unknown handling.
    """
    result = classify_pricing(
        price_text=price_text,
        summary=summary,
        title=title,
        source_name=source_name,
        source_url=source_url,
        location_name=location_name,
    )
    # For FREE the legacy callers expected the string "Free" not None
    legacy_price = result.price_text
    if result.cost_type == CostType.FREE and not legacy_price:
        legacy_price = "Free"
    return result.cost_type, legacy_price, result.reason
