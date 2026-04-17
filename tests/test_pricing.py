"""
Tests for the event pricing classifier.

Covers the pricing-extraction bug where many library/parks events with explicit
"registration fee" or "$X" text were being mis-classified as free.
"""

from __future__ import annotations

import pytest

from config.schema import CostType, PriceType
from enrichment.pricing import classify_pricing, infer_cost


# ---------------------------------------------------------------------------
# Free events
# ---------------------------------------------------------------------------

class TestFreeClassification:
    def test_explicit_free_admission_is_free(self):
        r = classify_pricing(
            summary="This is a free event. Free admission for all attendees.",
            title="Concert",
        )
        assert r.price_type == PriceType.FREE
        assert r.is_free is True
        assert r.cost_type == CostType.FREE

    def test_library_with_no_pricing_defaults_free(self):
        r = classify_pricing(
            source_name="Arlington County Public Library",
            summary="Join us for a storytime session.",
            title="Toddler Storytime",
        )
        assert r.price_type == PriceType.FREE
        assert r.is_free is True
        assert r.reason.startswith("library_default")

    def test_parks_with_no_pricing_defaults_free(self):
        r = classify_pricing(
            source_name="Fairfax County Park Authority",
            summary="Join a nature walk with a park ranger.",
            title="Ranger Walk",
        )
        assert r.price_type == PriceType.FREE
        assert r.is_free is True
        assert "public" in r.reason or "library" in r.reason

    def test_zero_cost_normalized_to_free(self):
        r = classify_pricing(price_text="$0", title="Concert")
        assert r.price_type == PriceType.FREE
        assert r.is_free is True

    def test_no_charge_is_free(self):
        r = classify_pricing(
            summary="Bring your family — no charge for this event.",
            title="Movie Night",
        )
        assert r.price_type == PriceType.FREE
        assert r.is_free is True


# ---------------------------------------------------------------------------
# Paid events — core bug coverage
# ---------------------------------------------------------------------------

class TestPaidClassification:
    def test_library_with_registration_fee_is_paid(self):
        """Core bug: library event with 'registration fee' used to be free."""
        r = classify_pricing(
            source_name="Fairfax County Public Library",
            source_url="https://librarycalendar.fairfaxcounty.gov/event/1234",
            location_name="Main Library",
            summary="Summer STEM Camp. Registration fee: $25 per child.",
            title="Summer STEM Camp",
        )
        assert r.price_type == PriceType.PAID
        assert r.is_free is False
        assert r.cost_type == CostType.PAID
        assert r.pricing_summary and "Registration fee" in r.pricing_summary
        assert "$25" in r.pricing_summary
        assert r.registration_required is True

    def test_parks_with_registration_fee_is_paid(self):
        """Parks events can also be paid — parks default shouldn't block fees."""
        r = classify_pricing(
            source_name="Fairfax County Park Authority",
            location_name="Riverbend Park, Great Falls, VA",
            summary="Registration fee: $20 per child. Advance registration required.",
            title="Birds of Prey Workshop",
        )
        assert r.price_type == PriceType.PAID
        assert r.is_free is False
        assert r.registration_required is True

    def test_dollar_amount_alone_is_paid(self):
        r = classify_pricing(
            source_name="Some Venue",
            summary="$15 per person.",
            title="Workshop",
        )
        assert r.price_type == PriceType.PAID
        assert r.is_free is False

    def test_library_with_fee_word_is_paid(self):
        """Weak paid signal ('fee' alone) should override the library default."""
        r = classify_pricing(
            source_name="Loudoun County Public Library",
            summary="A fee applies for this craft workshop.",
            title="Craft Workshop",
        )
        assert r.price_type == PriceType.PAID
        assert r.is_free is False
        assert r.reason == "weak_paid_signal"

    def test_library_ticket_required_is_paid(self):
        r = classify_pricing(
            source_name="Loudoun County Public Library",
            summary="Ticket required. Pick up at the circulation desk.",
            title="Author Talk",
        )
        assert r.price_type == PriceType.PAID
        assert r.is_free is False

    def test_cost_field_with_dollar_is_paid(self):
        r = classify_pricing(
            price_text="Cost: $12",
            source_name="Arts Center",
            title="Pottery Class",
        )
        assert r.price_type == PriceType.PAID
        assert r.is_free is False

    def test_price_text_only_numeric(self):
        r = classify_pricing(price_text="$8", title="Class")
        assert r.price_type == PriceType.PAID
        assert r.is_free is False

    def test_includes_admission_is_paid(self):
        r = classify_pricing(
            summary="Program fee includes admission to the museum.",
            title="Family Workshop",
        )
        assert r.price_type == PriceType.PAID
        assert r.is_free is False


# ---------------------------------------------------------------------------
# Mixed pricing — members / non-members
# ---------------------------------------------------------------------------

class TestMixedPricing:
    def test_members_free_non_members_paid_is_mixed(self):
        r = classify_pricing(
            source_name="Childrens Museum",
            summary="Members free. Non-members $12.",
            title="Exhibit",
        )
        assert r.price_type == PriceType.MIXED
        assert r.is_free is False
        assert r.cost_type == CostType.PAID

    def test_free_for_members_is_mixed(self):
        r = classify_pricing(
            source_name="Museum",
            summary="Free for members, $15 for non-members.",
            title="Lecture",
        )
        assert r.price_type == PriceType.MIXED
        assert r.is_free is False

    def test_free_with_paid_admission_is_mixed(self):
        r = classify_pricing(
            source_name="Zoo",
            summary="Special event free with paid admission.",
            title="Animal Encounter",
        )
        assert r.price_type == PriceType.MIXED
        assert r.is_free is False

    def test_free_event_with_tickets_required_is_conflicting(self):
        r = classify_pricing(
            source_name="Venue",
            summary="This is a free event but tickets required.",
            title="Concert",
        )
        # When both signals are present but no $ amount, we mark it mixed so
        # the UI can surface the ticket requirement instead of claiming free.
        assert r.price_type == PriceType.MIXED
        assert r.is_free is False


# ---------------------------------------------------------------------------
# Donation-based
# ---------------------------------------------------------------------------

class TestDonation:
    def test_suggested_donation_is_donation(self):
        r = classify_pricing(
            source_name="Garden",
            summary="Suggested donation: $5.",
            title="Volunteer Day",
        )
        assert r.price_type == PriceType.DONATION
        assert r.is_free is False
        assert r.cost_type == CostType.SUGGESTED_DONATION

    def test_pay_what_you_can(self):
        r = classify_pricing(
            source_name="Theater",
            summary="Pay what you can at the door.",
            title="Matinee",
        )
        assert r.price_type == PriceType.DONATION

    def test_free_with_optional_donation(self):
        r = classify_pricing(
            source_name="Museum",
            summary="Free with optional donation appreciated.",
            title="Exhibit",
        )
        assert r.price_type == PriceType.DONATION


# ---------------------------------------------------------------------------
# Registration required (no fee)
# ---------------------------------------------------------------------------

class TestRegistrationRequired:
    def test_registration_required_without_fee_is_unknown(self):
        r = classify_pricing(
            source_name="Unknown Venue",
            summary="Advance registration required for this program.",
            title="Class",
        )
        assert r.price_type == PriceType.UNKNOWN
        assert r.is_free is None
        assert r.registration_required is True

    def test_registration_required_at_library_is_still_free(self):
        r = classify_pricing(
            source_name="Loudoun County Public Library",
            summary="Registration required online.",
            title="Storytime",
        )
        # Registration-required alone doesn't flip the library default.
        assert r.price_type == PriceType.FREE
        assert r.is_free is True
        assert r.registration_required is True

    def test_registration_url_plus_content_flags_registration(self):
        r = classify_pricing(
            source_name="Venue",
            summary="Kids workshop with painting supplies.",
            title="Kids Workshop",
            registration_url="https://example.com/register",
        )
        assert r.registration_required is True


# ---------------------------------------------------------------------------
# Unknown fallback — do NOT default to free
# ---------------------------------------------------------------------------

class TestUnknownFallback:
    def test_commercial_source_with_no_pricing_is_unknown(self):
        r = classify_pricing(
            source_name="Private Art Studio",
            summary="Come join us for a fun morning.",
            title="Drop-in Class",
        )
        assert r.price_type == PriceType.UNKNOWN
        assert r.is_free is None
        assert r.cost_type == CostType.UNKNOWN

    def test_missing_price_does_not_default_to_free(self):
        r = classify_pricing(
            source_name="Some Studio",
            title="Event",
        )
        assert r.is_free is None
        assert r.price_type == PriceType.UNKNOWN

    def test_no_context_returns_unknown(self):
        r = classify_pricing()
        assert r.price_type == PriceType.UNKNOWN
        assert r.is_free is None


# ---------------------------------------------------------------------------
# Pricing details preserved
# ---------------------------------------------------------------------------

class TestPricingDetails:
    def test_pricing_summary_preserved(self):
        r = classify_pricing(
            summary="Registration fee: $15 per child for the afternoon program.",
            title="Afternoon Program",
        )
        assert r.pricing_summary is not None
        assert "$15" in r.pricing_summary
        assert "per child" in r.pricing_summary.lower() or "registration" in r.pricing_summary.lower()

    def test_registration_fee_text_captured(self):
        r = classify_pricing(
            summary="Registration fee: $40 per family.",
            title="Family Trivia Night",
        )
        assert r.registration_fee_text is not None
        assert "$40" in r.registration_fee_text
        assert r.registration_required is True

    def test_extracted_price_text_preserved_from_input(self):
        r = classify_pricing(
            price_text="Adults $15, Children $8",
            source_name="Theater",
            title="Show",
        )
        assert r.price_type == PriceType.PAID
        assert r.extracted_price_text == "Adults $15, Children $8"

    def test_reason_and_matched_patterns_populated(self):
        r = classify_pricing(
            summary="Registration fee: $25.",
            title="Camp",
        )
        assert r.reason == "strong_paid_signal"
        assert r.matched_patterns, "matched_patterns should not be empty"
        assert any("registration fee" in p.lower() for p in r.matched_patterns)


# ---------------------------------------------------------------------------
# Legacy infer_cost wrapper
# ---------------------------------------------------------------------------

class TestLegacyInferCost:
    def test_wrapper_returns_tuple(self):
        ct, txt, reason = infer_cost(
            price_text=None,
            summary="Registration fee: $10",
            title="Class",
        )
        assert ct == CostType.PAID
        assert txt is not None
        assert reason == "strong_paid_signal"

    def test_wrapper_free_has_price_text_free(self):
        ct, txt, reason = infer_cost(
            price_text=None,
            source_name="Fairfax County Public Library",
            title="Storytime",
        )
        assert ct == CostType.FREE
        assert txt == "Free" or "free" in (txt or "").lower()


# ---------------------------------------------------------------------------
# End-to-end via normalize_record
# ---------------------------------------------------------------------------

class TestNormalizeRecordIntegration:
    def _raw(self, **overrides):
        base = {
            "title": "Test Event",
            "source_name": "Fairfax County Public Library",
            "source_url": "https://library.example.com/event/1",
            "start_text": "2026-05-15 10:00",
            "location_text": "Main Library, Fairfax, VA",
            "summary_text": None,
            "price_text": None,
        }
        base.update(overrides)
        return base

    def test_library_registration_fee_event_is_paid(self):
        from enrichment.normalize import normalize_record
        e = normalize_record(
            self._raw(
                title="Summer Camp",
                summary_text="Summer STEM camp. Registration fee: $25 per child.",
            )
        )
        assert e is not None
        assert e.is_free is False
        assert e.price_type == PriceType.PAID
        assert e.cost_type == CostType.PAID
        assert e.pricing_summary and "$25" in e.pricing_summary
        assert e.registration_required is True
        assert "free" not in e.tags

    def test_library_plain_storytime_stays_free(self):
        from enrichment.normalize import normalize_record
        e = normalize_record(
            self._raw(
                title="Toddler Storytime",
                summary_text="Songs and stories for toddlers and caregivers.",
            )
        )
        assert e is not None
        assert e.is_free is True
        assert e.price_type == PriceType.FREE
        assert "free" in e.tags

    def test_commercial_event_with_no_pricing_is_unknown(self):
        from enrichment.normalize import normalize_record
        e = normalize_record(
            self._raw(
                source_name="Private Arts Studio",
                source_url="https://artsstudio.example.com/class/3",
                title="Pottery Class",
                location_text="Arts Studio, Ashburn, VA",
                summary_text="Join us for an afternoon of pottery.",
            )
        )
        assert e is not None
        assert e.is_free is None
        assert e.price_type == PriceType.UNKNOWN
        assert e.cost_type == CostType.UNKNOWN
        assert "free" not in e.tags


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
