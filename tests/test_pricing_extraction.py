"""
Tests for pricing-text extraction on the scraping side.

These exercise the scraper and resolver helpers that feed `price_text` into
normalize_record(). Extraction coverage is what keeps paid events from
reaching the classifier with no signals to work with.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from scrapers.fairfax_parks import _extract_price_text
from seed_discovery.resolver import _clean_price_text, _extract_cost_from_html


# ---------------------------------------------------------------------------
# Fairfax Parks card-description extraction
# ---------------------------------------------------------------------------

class TestFairfaxParksPriceExtraction:
    def test_dollar_per_child(self):
        s = "5:30PM, (ages 5-10) Nature walk. Registration fee: $12 per child."
        assert _extract_price_text(s) == "Registration fee: $12 per child"

    def test_cost_colon_dollar(self):
        s = "10:00AM, Birds of Prey program. Cost: $20."
        assert _extract_price_text(s) == "Cost: $20"

    def test_free_admission(self):
        s = "Join us for a guided tour. Free admission."
        out = _extract_price_text(s)
        assert out is not None
        assert "free" in out.lower()

    def test_no_pricing_returns_none(self):
        s = "3:00PM, (ages 3-5) Storytime at the farm."
        assert _extract_price_text(s) is None

    def test_members_free(self):
        s = "Special tour. Members free, non-members $10."
        out = _extract_price_text(s)
        assert out is not None
        assert "free" in out.lower() or "$" in out


# ---------------------------------------------------------------------------
# Seed resolver HTML extraction — CTA / widget / labelled text
# ---------------------------------------------------------------------------

class TestResolverHtmlExtraction:
    def test_dedicated_price_class(self):
        html = """
        <div class="event-detail">
          <h1>Pottery Workshop</h1>
          <div class="event-cost">$25 per person</div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        assert _extract_cost_from_html(soup) == "$25 per person"

    def test_tribe_events_cost_container(self):
        html = '<div class="tribe-events-cost">$15</div>'
        soup = BeautifulSoup(html, "lxml")
        assert _extract_cost_from_html(soup) == "$15"

    def test_labelled_registration_fee_in_body(self):
        html = """
        <div class="content">
          <p>Join our science camp for a week of fun.</p>
          <p>Registration fee: $150 per child. All materials included.</p>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        out = _extract_cost_from_html(soup)
        assert out is not None
        assert "Registration fee" in out
        assert "$150" in out

    def test_cta_button_price_nearby(self):
        html = """
        <div class="event-info">
          <p>Storytelling night at 7pm — $8 at the door.</p>
          <a class="btn" href="/register">Register now</a>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        out = _extract_cost_from_html(soup)
        assert out is not None
        assert "$8" in out

    def test_no_price_returns_none(self):
        html = """
        <div>
          <h1>Free Community Event</h1>
          <p>Join us at the park for music and games.</p>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        out = _extract_cost_from_html(soup)
        # Either returns "Free ..." snippet or None — just don't invent a price.
        assert out is None or "$" not in out


# ---------------------------------------------------------------------------
# Resolver price-text cleanup
# ---------------------------------------------------------------------------

class TestCleanPriceText:
    def test_short_text_preserved(self):
        assert _clean_price_text("$10 per child") == "$10 per child"

    def test_long_text_salvages_snippet(self):
        long = (
            "Lorem ipsum dolor sit amet " * 20
            + " Registration fee: $25 per child is required for this program."
        )
        out = _clean_price_text(long)
        assert out is not None
        assert "$25" in out or "Registration fee" in out

    def test_none_input(self):
        assert _clean_price_text(None) is None

    def test_empty_string(self):
        assert _clean_price_text("   ") is None
