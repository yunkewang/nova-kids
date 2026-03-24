"""
Family relevance classification for the NoVA Kids event pipeline.

Determines whether a scraped event belongs in the family/children's activities
feed vs. being an adult-oriented public service that happens to be hosted at a
family-friendly venue (e.g. free tax prep at a library).

Public API:
    classify_family_relevance(event_data, tags) -> dict
        Returns {"score": float, "label": str, "reasons": list[str]}

    PUBLISH_THRESHOLD: float — events below this score are excluded from the feed.

Design:
  - Starts at a mildly positive base (0.45) so unknown events default to
    "publish unless clearly wrong."
  - Source stance nudges the base up for sources whose events are almost always
    family-relevant (library children's programs, park authority, etc.).
  - Strong age-group tags (toddler, preschool, elementary, all_ages) push the
    score higher.
  - Explicit family / child-audience keywords push the score higher.
  - Adult-service keyword matches apply a large negative penalty (-0.50 each).
  - Rescue patterns partially counteract an adult-service penalty when the same
    event is clearly framed for parents / families with children.
  - Threshold = 0.30 — events that reach this score are published; below it
    they are written to data/manual_review/excluded_non_family_events.json.
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Strong NEGATIVE signals — adult service / civic / non-family events
# ---------------------------------------------------------------------------
# Each tuple: (reason_label, compiled_regex)

_ADULT_SERVICE_RULES: list[tuple[str, re.Pattern]] = [
    # ── Tax / VITA ──────────────────────────────────────────────────────────
    ("tax_services", re.compile(
        r"\btax(?:es|ation|payer)?\b"
        r"|\bvita\b"
        r"|\bincome\s+tax\b"
        r"|\btax\s+(?:prep(?:aration)?|assistance|filing|help|return|season)\b"
        r"|\bfree\s+tax\b",
        re.IGNORECASE,
    )),

    # ── Legal clinics ────────────────────────────────────────────────────────
    ("legal_clinic", re.compile(
        r"\blegal\s+(?:clinic|aid|services?|advice|assistance|consult\w*)\b"
        r"|\bfree\s+legal\b"
        r"|\battorney\s+clinic\b"
        r"|\bpro\s+bono\b",
        re.IGNORECASE,
    )),

    # ── Employment / resume / job fair ───────────────────────────────────────
    ("employment", re.compile(
        r"\bresume\s+(?:workshop|review|writing|help|assistance|building|prep)\b"
        r"|\bjob\s+(?:fair|search|readiness|training|hunt)\b"
        r"|\bcareer\s+(?:fair|workshop|readiness|coaching)(?!\s+for\s+(?:teen|youth|student))\b"
        r"|\bemployment\s+(?:workshop|readiness|services?|assistance)\b"
        r"|\binterview\s+(?:prep|skills|practice)\b"
        r"|\bwork\s+readiness\b"
        r"|\bworkplace\s+ready\b",
        re.IGNORECASE,
    )),

    # ── Financial / retirement / mortgage ───────────────────────────────────
    ("financial_services", re.compile(
        r"\bretirement\s+(?:planning|workshop|seminar)\b"
        r"|\bmedicare\s+(?:enrollment|information|workshop|seminar|help)\b"
        r"|\bsocial\s+security\s+(?:benefits?|workshop|seminar)\b"
        r"|\bhomebuyer\s+(?:workshop|seminar|class|education)\b"
        r"|\bcredit\s+(?:counseling|repair|workshop|score)\b"
        r"|\bfinancial\s+(?:planning|literacy\s+for\s+adults?|independence)\b"
        r"|\binvesting\s+(?:basics|workshop|seminar)\b"
        r"|\bwealth\s+management\b"
        r"|\bdebt\s+(?:management|reduction|workshop)\b"
        r"|\binsurance\s+(?:workshop|seminar|open\s+enrollment)\b"
        r"|\bbudget(?:ing)?\s+(?:workshop|seminar)\b",
        re.IGNORECASE,
    )),

    # ── Civic / board / policy meetings ─────────────────────────────────────
    ("civic_admin", re.compile(
        r"\bboard\s+(?:meeting|of\s+supervisors|of\s+directors)\b"
        r"|\bcommittee\s+meeting\b"
        r"|\bpublic\s+hearing\b"
        r"|\btown\s+hall\s+(?:meeting)?\b"
        r"|\bpolicy\s+(?:forum|meeting|discussion)\b"
        r"|\bplanning\s+commission\b"
        r"|\bcounty\s+council\s+meeting\b"
        r"|\bbudget\s+(?:hearing|session)\b",
        re.IGNORECASE,
    )),

    # ── Voter / civic engagement ─────────────────────────────────────────────
    ("voter_civic", re.compile(
        r"\bvoter\s+(?:registration|information|assistance)\b"
        r"|\bcivic\s+engagement\b"
        r"|\bget\s+out\s+the\s+vote\b",
        re.IGNORECASE,
    )),

    # ── Adult support groups (non-family) ────────────────────────────────────
    ("adult_support_group", re.compile(
        r"\bgrief\s+(?:support\s+)?group\b"
        r"|\baddiction\s+(?:support\s+)?group\b"
        r"|\brecovery\s+(?:support\s+)?group\b"
        r"|\bsubstance\s+abuse\s+(?:support\s+)?group\b"
        r"|\baa\s+meeting\b"
        r"|\balcoholics?\s+anonymous\b"
        r"|\bnarcotics?\s+anonymous\b"
        r"|\bsupport\s+group\b",   # general; rescue patterns override if family-specific
        re.IGNORECASE,
    )),

    # ── Adult health / screening services ───────────────────────────────────
    # Negative lookahead prevents matching "for kids/children/family/student"
    ("adult_health_services", re.compile(
        r"\bblood\s+(?:drive|pressure\s+screening|donation)\b"
        r"|\bcholesterol\s+screening\b"
        r"|\bhealth\s+(?:screening|fair)(?!\s+for\s+(?:kid|child|student|famil))\b"
        r"|\bvaccin(?:e|ation|ations?)\s+(?:clinic|drive|event)"
        r"(?!\s+for\s+(?:kid|child|famil|student))\b"
        r"|\bdental\s+(?:screening|clinic)(?!\s+for\s+(?:kid|child|student|famil))\b",
        re.IGNORECASE,
    )),

    # ── Business networking ──────────────────────────────────────────────────
    ("business_networking", re.compile(
        r"\bbusiness\s+(?:network\w*|mixer|development|owner)\b"
        r"|\bentrepreneur\s+(?:workshop|seminar|network\w*)\b"
        r"|\bstartup\s+(?:network|event|meeting)\b"
        r"|\bchamber\s+of\s+commerce\b"
        r"|\bprofessional\s+(?:development|network\w*)"
        r"(?!\s+for\s+(?:teen|youth|student|educator|teacher))\b",
        re.IGNORECASE,
    )),

    # ── Adult book club / lecture ────────────────────────────────────────────
    ("adult_book_club", re.compile(
        r"\badult\s+book\s+(?:club|discussion)\b"
        r"|\bbrown\s+bag\s+(?:lecture|discussion|seminar)\b",
        re.IGNORECASE,
    )),

    # ── Passport / government services ──────────────────────────────────────
    ("government_services", re.compile(
        r"\bpassport\s+(?:application|assistance|help|services?)\b"
        r"|\bdmv\s+(?:services?|appointment)\b"
        r"|\bnotary\s+(?:services?|public)\b",
        re.IGNORECASE,
    )),
]

# ---------------------------------------------------------------------------
# Strong POSITIVE signals — clearly child / family-oriented
# ---------------------------------------------------------------------------

_FAMILY_POSITIVE_RULES: list[tuple[str, re.Pattern]] = [
    ("children_audience", re.compile(
        r"\bchildren\b|\bkids?\b|\bchild\b"
        r"|\btoddlers?\b|\bbab(?:y|ies)\b|\binfants?\b"
        r"|\bpreschoolers?\b|\belementary\s+(?:student|school|age)\b"
        r"|\byoung\s+readers?\b|\blittle\s+ones\b|\bwee\s+ones\b",
        re.IGNORECASE,
    )),
    ("family_framing", re.compile(
        r"\bfamily\s+(?:event|night|day|fun|friendly|program|activity|activities)\b"
        r"|\bfor\s+(?:the\s+)?(?:whole\s+)?family\b"
        r"|\bfamilies\s+(?:welcome|invited|with\s+children)\b"
        r"|\bfamily.?friendly\b"
        r"|\bbring\s+(?:the\s+)?family\b",
        re.IGNORECASE,
    )),
    ("storytime", re.compile(
        r"\bstory\s*time\b|\bread\s+aloud\b|\blap\s*sit\b|\bbaby\s+lap\b",
        re.IGNORECASE,
    )),
    ("youth_program", re.compile(
        r"\bcamp\b|\bafter[\s-]school\b|\bschool[\s-]age\b"
        r"|\byouth\s+(?:program|club|event|class|workshop)\b"
        r"|\bchildren'?s?\s+(?:program|class|event|workshop|activity)\b"
        r"|\bkids?\s+(?:club|class|program|event|workshop|zone|corner|night)\b"
        r"|\bjunior\s+(?:ranger|scientist|naturalist|chef)\b",
        re.IGNORECASE,
    )),
    ("parent_caregiver_family", re.compile(
        r"\bparent(?:ing)?\s+(?:workshop|class|seminar|webinar|group|education)\b"
        r"|\bcaregivers?\s+(?:and\s+)?(?:children|kids?|toddlers?|babies?|infants?)\b"
        r"|\bearly\s+childhood\b|\bchild\s+development\b"
        r"|\bschool\s+readiness\b"
        r"|\bpreschool\s+(?:enrollment|fair|open\s+house)\b"
        r"|\bfamily\s+(?:resource|learning|open\s+house|literacy)\b"
        r"|\bchild\s+find\b",
        re.IGNORECASE,
    )),
]

# ---------------------------------------------------------------------------
# Moderate POSITIVE signals — kid-friendly activities (not always labeled for children)
# ---------------------------------------------------------------------------

_ACTIVITY_POSITIVE_RULES: list[tuple[str, re.Pattern]] = [
    ("stem_activity", re.compile(
        r"\bstem\b|\bscience\s+(?:experiment|exploration|fair|club)\b"
        r"|\bcoding\s+(?:club|class|for\s+kids)\b|\brobotics\b"
        r"|\bmaker\s+(?:space|faire|fair)\b",
        re.IGNORECASE,
    )),
    ("arts_crafts", re.compile(
        r"\bcrafts?\b|\bart\s+(?:class|project|activity|for\s+kids)\b"
        r"|\bpainting\b|\bdrawing\b|\bcoloring\b",
        re.IGNORECASE,
    )),
    ("nature_outdoor_family", re.compile(
        r"\bnature\s+(?:walk|hike|exploration|play|program|center)\b"
        r"|\bscavenger\s+hunt\b|\bbird\s+watch\w*\b"
        r"|\bpetting\s+(?:zoo|farm)\b|\bfarm\s+(?:visit|tour|animals?)\b",
        re.IGNORECASE,
    )),
    ("play_activity", re.compile(
        r"\bopen\s+gym\b|\bplay\s*(?:group|date|time|space)\b"
        r"|\bsensory\s+(?:play|friendly|room)\b"
        r"|\bgymnastics\b|\bswim\s+(?:lesson|class)\b|\bskating\b",
        re.IGNORECASE,
    )),
    ("seasonal_family", re.compile(
        r"\begg\s+hunt\b|\bhalloween\s+(?:for\s+kids|event|parade|party)\b"
        r"|\bsanta\s+(?:visit|photos?|comes)\b|\bcarnival\b"
        r"|\bholiday\s+(?:event|party|crafts?|parade)\b",
        re.IGNORECASE,
    )),
]

# ---------------------------------------------------------------------------
# RESCUE patterns — override an adult-service hit when family context is clear
# ---------------------------------------------------------------------------

_FAMILY_RESCUE_RULES: list[tuple[str, re.Pattern]] = [
    ("parenting_context", re.compile(
        r"\bfor\s+(?:parent|mom|dad|caregiver|guardian)s?\b"
        r"|\bparenting\b"
        r"|\bparent\s+(?:and|&|with)\s+(?:child|kid|baby|toddler)\b"
        r"|\bchildren\s+(?:welcome|must\s+(?:be\s+)?accompanied)\b",
        re.IGNORECASE,
    )),
    ("child_health_family", re.compile(
        r"\bchild(?:ren)?'?s?\s+(?:health|dental|vision|vaccination|immuniz)\b"
        r"|\bback[\s-]to[\s-]school\s+(?:health|physicals?|shots?)\b"
        r"|\bkids?\s+(?:health|dental|vision|vaccine)\b"
        r"|\bpediatric\b",
        re.IGNORECASE,
    )),
    ("support_family_specific", re.compile(
        r"\bsupport\s+group\s+for\s+(?:parent|family|mom|dad|caregiver|guardian)\b"
        r"|\bparent\s+support\b|\bfamily\s+support\b",
        re.IGNORECASE,
    )),
]

# ---------------------------------------------------------------------------
# Source-name stance (added to base score before keyword scoring)
# ---------------------------------------------------------------------------
# Library children's programs and park authorities start slightly positive.
# This helps events with sparse descriptions from well-known family sources.

_SOURCE_NAME_STANCE: dict[str, float] = {
    "Fairfax County Public Library":        +0.20,
    "Arlington Public Library":             +0.20,
    "Loudoun County Public Library":        +0.20,
    "Alexandria Library":                   +0.20,
    "Fairfax County Park Authority":        +0.15,
    "Arlington County Parks & Recreation":  +0.10,
    "NOVA Parks":                           +0.08,
}

# ---------------------------------------------------------------------------
# Scoring weights
# ---------------------------------------------------------------------------

_BASE_SCORE          = 0.45   # neutral default (publish unless clearly wrong)
_ADULT_PENALTY       = -0.50  # per matching adult-service rule
_FAMILY_BOOST        = +0.25  # per matching family-positive rule (capped at 1 match)
_ACTIVITY_BOOST      = +0.12  # per matching activity rule (capped at 1 match)
_RESCUE_BOOST        = +0.35  # per matching rescue rule (capped at 1 match)
_AGE_TAG_BOOST       = +0.30  # family age-group tags (toddler, preschool, elementary, all_ages)
_TEEN_TAG_BOOST      = +0.10  # teen tag (positive, but less so than young-child tags)

# ---------------------------------------------------------------------------
# Publish threshold
# ---------------------------------------------------------------------------

PUBLISH_THRESHOLD = 0.30  # events below this score are excluded from the family feed


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

def classify_family_relevance(
    event_data: dict[str, Any],
    tags: list[str],
) -> dict[str, Any]:
    """
    Classify an event's relevance to families with children.

    Parameters
    ----------
    event_data : dict
        Normalized event dict (may still be pre-Pydantic at enrich time).
    tags : list[str]
        Already-derived tag list (from derive_tags).

    Returns
    -------
    dict with:
        score   (float 0–1)    — family relevance score
        label   (str)          — "family" | "parent_oriented" | "neutral" | "adult_service"
        reasons (list[str])    — matched rules, for debug/exclusion report
    """
    combined = " ".join(filter(None, [
        event_data.get("title", ""),
        event_data.get("summary", ""),
    ]))
    tag_set = set(tags)
    source_name: str = event_data.get("source_name", "")

    # ── Base score + source stance ──────────────────────────────────────────
    score = _BASE_SCORE + _SOURCE_NAME_STANCE.get(source_name, 0.0)
    reasons: list[str] = []

    # ── Age-group tag bonus ─────────────────────────────────────────────────
    family_age_tags = {"toddler", "preschool", "elementary", "all_ages"}
    matched_age_tags = tag_set & family_age_tags
    if matched_age_tags:
        score += _AGE_TAG_BOOST
        reasons.append("age_tag:" + "+".join(sorted(matched_age_tags)))
    elif "teen" in tag_set:
        score += _TEEN_TAG_BOOST
        reasons.append("age_tag:teen")

    # ── Strong family-positive signals (take the first match only) ──────────
    for rule_name, pattern in _FAMILY_POSITIVE_RULES:
        if pattern.search(combined):
            score += _FAMILY_BOOST
            reasons.append(f"pos:{rule_name}")
            break

    # ── Activity-positive signals (take the first match only) ───────────────
    for rule_name, pattern in _ACTIVITY_POSITIVE_RULES:
        if pattern.search(combined):
            score += _ACTIVITY_BOOST
            reasons.append(f"activity:{rule_name}")
            break

    # ── Adult-service negative signals (each match penalises separately) ────
    adult_hits: list[str] = []
    for rule_name, pattern in _ADULT_SERVICE_RULES:
        if pattern.search(combined):
            adult_hits.append(rule_name)

    if adult_hits:
        score += _ADULT_PENALTY * len(adult_hits)
        for h in adult_hits:
            reasons.append(f"neg:{h}")

        # Rescue: family/parenting context can partially counter the penalty
        for rule_name, pattern in _FAMILY_RESCUE_RULES:
            if pattern.search(combined):
                score += _RESCUE_BOOST
                reasons.append(f"rescue:{rule_name}")
                break  # one rescue is enough

    # ── Clamp ───────────────────────────────────────────────────────────────
    score = round(min(max(score, 0.0), 1.0), 4)

    # ── Label ───────────────────────────────────────────────────────────────
    if score >= 0.65:
        label = "family"
    elif score >= 0.45:
        label = "parent_oriented"
    elif score >= PUBLISH_THRESHOLD:
        label = "neutral"
    else:
        label = "adult_service"

    return {"score": score, "label": label, "reasons": reasons}
