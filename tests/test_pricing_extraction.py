"""
Tests for pricing-text extraction on the scraping side.

These exercise the scraper and resolver helpers that feed `price_text` into
normalize_record(). Extraction coverage is what keeps paid events from
reaching the classifier with no signals to work with.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from scrapers.fairfax_parks import (
    _extract_price_from_detail_html,
    _extract_price_text,
)
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

class TestFairfaxParksDetailPriceExtraction:
    """
    Detail-page extraction tests for Fairfax Parks.

    The list calendar rarely carries pricing; the actual "PRICE REGISTRATION
    $115.00" block lives on the per-event detail page. _extract_price_from_detail_html
    must salvage that signal so paid workshops don't fall back to the
    parks "default free" rule.
    """

    def test_price_registration_label_dollar(self):
        # Mirrors the Fairfax Parks "PRICE / REGISTRATION $115.00" layout.
        html = """
        <html><body>
          <h1>Colored Pencil and Acrylic Workshop</h1>
          <div class="event-info">
            <div class="label">PRICE</div>
            <div class="value">REGISTRATION $115.00</div>
          </div>
        </body></html>
        """
        out = _extract_price_from_detail_html(html)
        assert out is not None
        assert "$115.00" in out

    def test_cost_label_with_dollar(self):
        html = """
        <html><body>
          <h1>Pottery Class</h1>
          <p>Cost: $45 per session.</p>
        </body></html>
        """
        out = _extract_price_from_detail_html(html)
        assert out is not None
        assert "$45" in out

    def test_registration_fee_label(self):
        html = """
        <html><body>
          <p>Registration fee: $80 per child.</p>
        </body></html>
        """
        out = _extract_price_from_detail_html(html)
        assert out is not None
        assert "$80" in out
        assert "egistration" in out.lower() or "$80" in out

    def test_free_admission_text(self):
        html = """
        <html><body>
          <h1>Family Nature Walk</h1>
          <p>Free admission. All ages welcome.</p>
        </body></html>
        """
        out = _extract_price_from_detail_html(html)
        assert out is not None
        assert "free" in out.lower()

    def test_no_pricing_returns_none(self):
        html = """
        <html><body>
          <h1>Park Ranger Talk</h1>
          <p>Join us at the visitor center.</p>
        </body></html>
        """
        out = _extract_price_from_detail_html(html)
        assert out is None

    def test_empty_html_returns_none(self):
        assert _extract_price_from_detail_html("") is None
        assert _extract_price_from_detail_html(None) is None  # type: ignore[arg-type]

    def test_script_content_ignored(self):
        # Dollar amounts inside <script>/<style> shouldn't leak through
        html = """
        <html><body>
          <script>var price = "$999";</script>
          <h1>Free Concert in the Park</h1>
          <p>Bring a blanket.</p>
        </body></html>
        """
        out = _extract_price_from_detail_html(html)
        # Either matches "Free" copy or returns None — must NOT return $999.
        assert out is None or "$999" not in out

    def test_price_registration_split_lines(self):
        # Some Fairfax pages render PRICE on one row and the dollar amount on
        # the next. After flattening, the "registration $115" pattern still wins.
        html = """
        <html><body>
          <table>
            <tr><th>Date</th><td>April 18, 2026</td></tr>
            <tr><th>Time</th><td>10am - 12pm</td></tr>
            <tr><th>Price</th><td>Registration $115.00</td></tr>
          </table>
        </body></html>
        """
        out = _extract_price_from_detail_html(html)
        assert out is not None
        assert "$115" in out


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
