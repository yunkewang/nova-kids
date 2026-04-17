"""
Tests for pricing-text extraction on the scraping side.

These exercise the scraper and resolver helpers that feed `price_text` into
normalize_record(). Extraction coverage is what keeps paid events from
reaching the classifier with no signals to work with.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from scrapers.detail_price import (
    extract_price_from_detail_html,
    fetch_detail_price,
)
from scrapers.fairfax_parks import (
    FairfaxParksAuthorityScraper,
    _extract_price_from_detail_html,  # re-exported for back-compat
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


class TestSharedDetailPriceExtraction:
    """
    Tests for the shared `extract_price_from_detail_html` used by all parks
    scrapers. Covers structured HTML selectors, the "Trainers on the Go"
    regression (title with no paid-program keywords but the detail page has
    "PRICE ... $12.00"), labelled blocks with unusual whitespace, and the
    refusal to return script-tag noise.
    """

    def test_trainers_on_the_go_regression(self):
        # The user-reported case: "Trainers on the Go" title has no
        # paid-program keywords, but the detail page clearly shows
        # "PRICE REGISTRATION $12.00".
        html = """
        <html><body>
          <h1>Trainers on the Go</h1>
          <div class="event-meta">
            <div class="label">DATE &amp; TIME</div>
            <div class="value">4/18/2026 - 2:30 pm to 4:00 pm</div>
            <div class="label">PRICE REGISTRATION</div>
            <div class="value">$12.00</div>
            <div class="label">LOCATION</div>
            <div class="value">Riverbend Park</div>
          </div>
        </body></html>
        """
        out = extract_price_from_detail_html(html)
        assert out is not None
        assert "$12.00" in out

    def test_drupal_field_price_selector(self):
        html = """
        <html><body>
          <article>
            <div class="field field--name-field-price">$45.00</div>
          </article>
        </body></html>
        """
        out = extract_price_from_detail_html(html)
        assert out is not None
        assert "$45" in out

    def test_event_cost_class_selector(self):
        html = '<html><body><div class="event-cost">$25 per child</div></body></html>'
        out = extract_price_from_detail_html(html)
        assert out is not None
        assert "$25" in out

    def test_itemprop_price_selector(self):
        html = '<html><body><span itemprop="price">$80.00</span></body></html>'
        out = extract_price_from_detail_html(html)
        assert out is not None
        assert "$80" in out

    def test_labelled_price_with_range(self):
        html = """
        <html><body>
          <p>Cost: $20 - $40 depending on materials.</p>
        </body></html>
        """
        out = extract_price_from_detail_html(html)
        assert out is not None
        assert "$20" in out

    def test_tuition_label(self):
        html = "<html><body><p>Tuition: $350 for the 6-week session.</p></body></html>"
        out = extract_price_from_detail_html(html)
        assert out is not None
        assert "$350" in out

    def test_admission_label(self):
        html = "<html><body><p>Admission: $8 for adults, $5 for children.</p></body></html>"
        out = extract_price_from_detail_html(html)
        assert out is not None
        assert "$" in out

    def test_no_pricing_returns_none(self):
        html = """
        <html><body>
          <h1>Nature Walk</h1>
          <p>Join us for a guided walk with a park ranger.</p>
        </body></html>
        """
        out = extract_price_from_detail_html(html)
        assert out is None

    def test_empty_and_none_inputs(self):
        assert extract_price_from_detail_html("") is None
        assert extract_price_from_detail_html(None) is None  # type: ignore[arg-type]

    def test_script_noise_ignored(self):
        html = """
        <html><body>
          <script>var sku_price = "$9999";</script>
          <style>.cost { color: red; }</style>
          <h1>Free Family Story Hour</h1>
          <p>Free admission for all ages.</p>
        </body></html>
        """
        out = extract_price_from_detail_html(html)
        # Must not return the $9999 from the script tag
        assert out is None or "$9999" not in out
        # Should return the free-admission snippet
        assert out is None or "free" in out.lower()

    def test_free_admission_preserved(self):
        html = """
        <html><body>
          <h1>Spring Bird Walk</h1>
          <p>This event is free to attend. Binoculars provided.</p>
        </body></html>
        """
        out = extract_price_from_detail_html(html)
        assert out is not None
        assert "free" in out.lower()

    def test_bare_dollar_fallback(self):
        html = """
        <html><body>
          <h1>Pottery Open Studio</h1>
          <p>Bring your own clay. $15 for firing, payable on site.</p>
        </body></html>
        """
        out = extract_price_from_detail_html(html)
        assert out is not None
        assert "$15" in out


class TestFairfaxParksAlwaysFetchDetail:
    """
    The user reported that "Trainers on the Go" at Fairfax County Park
    Authority was showing as Free even though its detail page lists
    "PRICE REGISTRATION $12.00". Root cause: the scraper's previous
    keyword gate (`_should_fetch_detail`) only fetched the detail page
    for titles matching "workshop/class/camp/lesson/...". "Trainers on
    the Go" matched none of those, so we never even looked at the detail
    page. These tests confirm the scraper now fetches the detail page
    for ANY card without pricing, regardless of title keywords.
    """

    def _make_card_html(self, title: str, href: str, desc: str) -> BeautifulSoup:
        html = f"""
        <div class="events-list views-row">
          <div class="date">Apr 18</div>
          <div class="calendar-title"><a href="{href}">{title}</a></div>
          <div class="calendar-description">{desc}</div>
        </div>
        """
        return BeautifulSoup(html, "lxml").select_one("div.events-list.views-row")

    def _patched_scraper(self, detail_html: str) -> FairfaxParksAuthorityScraper:
        """Build a scraper whose .get() returns a fixed detail-page body."""
        sc = FairfaxParksAuthorityScraper()
        sc._detail_fetches = 0

        class _Resp:
            def __init__(self, text: str):
                self.text = text
            def raise_for_status(self):
                return None

        # Replace .get() so tests never hit the network
        def fake_get(url: str, **kwargs):
            return _Resp(detail_html)

        sc.get = fake_get  # type: ignore[method-assign]
        return sc

    def test_fetches_detail_for_non_keyword_title(self):
        """Regression for the Trainers-on-the-Go bug."""
        detail_html = """
        <html><body>
          <h1>Trainers on the Go</h1>
          <div class="event-meta">
            <div>PRICE REGISTRATION</div>
            <div>$12.00</div>
          </div>
        </body></html>
        """
        sc = self._patched_scraper(detail_html)
        card = self._make_card_html(
            "Trainers on the Go",
            "/parks/riverbend/trainers-go/041826",
            "2:30PM, (8-Adult) Combine your interests with a guided Pokémon Go hike at Riverbend…",
        )
        record = sc._parse_card(card)
        assert record is not None
        assert record["price_text"] is not None, (
            "Expected detail-page price fetch to populate price_text"
        )
        assert "$12" in record["price_text"]
        assert sc._detail_fetches == 1

    def test_skips_detail_fetch_when_card_already_has_price(self):
        """Cheap path: card description carries pricing, no detail fetch needed."""
        sc = self._patched_scraper("<html><body>Should not be read</body></html>")
        card = self._make_card_html(
            "Birds of Prey Workshop",
            "/parks/riverbend/birds/041826",
            "10:00AM, Registration fee: $20 per child.",
        )
        record = sc._parse_card(card)
        assert record is not None
        assert "$20" in (record["price_text"] or "")
        assert sc._detail_fetches == 0

    def test_free_detail_page_still_fetched(self):
        """Free events: we still fetch so we don't guess wrong."""
        detail_html = (
            "<html><body><h1>Ranger Walk</h1>"
            "<p>Free admission. All ages welcome.</p></body></html>"
        )
        sc = self._patched_scraper(detail_html)
        card = self._make_card_html(
            "Ranger Walk",
            "/parks/riverbend/walk/041826",
            "10:00AM, Join us for a guided walk.",
        )
        record = sc._parse_card(card)
        assert record is not None
        assert sc._detail_fetches == 1
        assert record["price_text"] is not None
        assert "free" in record["price_text"].lower()


class TestFetchDetailPriceHelper:
    """Unit tests for the fetch_detail_price helper — exercises limits and error handling."""

    class _FakeScraper:
        def __init__(self, html: str | None = None, fail: bool = False):
            self._detail_fetches = 0
            self._html = html
            self._fail = fail
            self.calls: list[str] = []

        def get(self, url: str):
            self.calls.append(url)
            if self._fail:
                raise RuntimeError("network error")

            class Resp:
                text = self._html or ""
            return Resp()

    def test_extracts_price_from_fetched_html(self):
        html = '<html><body><div class="event-cost">$42</div></body></html>'
        sc = self._FakeScraper(html=html)
        out = fetch_detail_price(sc, "http://example.com/e/1")
        assert out is not None
        assert "$42" in out
        assert sc._detail_fetches == 1

    def test_limit_stops_further_fetches(self):
        sc = self._FakeScraper(html='<html><body>$5</body></html>')
        for _ in range(3):
            fetch_detail_price(sc, "http://example.com/x", limit=2)
        assert sc._detail_fetches == 2
        assert len(sc.calls) == 2

    def test_fetch_failure_returns_none(self):
        sc = self._FakeScraper(fail=True)
        out = fetch_detail_price(sc, "http://example.com/broken")
        assert out is None
        # Counter still increments so a broken URL doesn't cause infinite retries
        assert sc._detail_fetches == 1

    def test_no_pricing_in_html_returns_none(self):
        sc = self._FakeScraper(html="<html><body><p>Nothing to see here</p></body></html>")
        assert fetch_detail_price(sc, "http://example.com/e/2") is None


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
