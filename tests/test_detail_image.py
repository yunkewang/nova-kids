"""
Tests for image extraction from event detail pages (scrapers/detail_image.py).

Extraction covers what keeps event listings visually useful downstream:
normalize_record() already prefers raw["image_url"], so anything these
extractors return lands on the published event without further wiring.
"""

from __future__ import annotations

from scrapers.detail_image import (
    extract_image_from_detail_html,
    fetch_detail_image,
)


# ---------------------------------------------------------------------------
# extract_image_from_detail_html — tag priority and URL resolution
# ---------------------------------------------------------------------------

class TestExtractImageFromDetailHtml:
    BASE = "https://www.fairfaxcounty.gov/parks/event/some-hike"

    def test_og_image_absolute(self):
        html = (
            '<html><head>'
            '<meta property="og:image" content="https://cdn.example.com/photo.jpg"/>'
            '</head><body><h1>Hike</h1></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) == \
            "https://cdn.example.com/photo.jpg"

    def test_og_image_relative_resolved(self):
        html = (
            '<html><head>'
            '<meta property="og:image" content="/sites/default/files/hike.jpg"/>'
            '</head><body></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) == \
            "https://www.fairfaxcounty.gov/sites/default/files/hike.jpg"

    def test_og_image_wins_over_twitter_image(self):
        html = (
            '<html><head>'
            '<meta property="og:image" content="https://cdn.example.com/og.jpg"/>'
            '<meta name="twitter:image" content="https://cdn.example.com/tw.jpg"/>'
            '</head><body></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) == \
            "https://cdn.example.com/og.jpg"

    def test_twitter_image_fallback(self):
        html = (
            '<html><head>'
            '<meta name="twitter:image" content="https://cdn.example.com/tw.jpg"/>'
            '</head><body></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) == \
            "https://cdn.example.com/tw.jpg"

    def test_og_image_secure_url_fallback(self):
        html = (
            '<html><head>'
            '<meta property="og:image:secure_url" '
            'content="https://secure.example.com/event.png"/>'
            '</head><body></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) == \
            "https://secure.example.com/event.png"

    def test_missing_tags_returns_none(self):
        html = "<html><head><title>No image here</title></head><body></body></html>"
        assert extract_image_from_detail_html(html, self.BASE) is None

    def test_none_html_returns_none(self):
        assert extract_image_from_detail_html(None, self.BASE) is None

    def test_data_uri_rejected(self):
        html = (
            '<html><head>'
            '<meta property="og:image" '
            'content="data:image/gif;base64,R0lGODlhAQABAAAAACw="/>'
            '</head><body></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) is None

    def test_data_uri_skipped_for_next_usable_tag(self):
        html = (
            '<html><head>'
            '<meta property="og:image" '
            'content="data:image/gif;base64,R0lGODlhAQABAAAAACw="/>'
            '<meta name="twitter:image" content="https://cdn.example.com/real.jpg"/>'
            '</head><body></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) == \
            "https://cdn.example.com/real.jpg"

    def test_favicon_rejected(self):
        html = (
            '<html><head>'
            '<meta property="og:image" content="/favicon.ico"/>'
            '</head><body></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) is None

    def test_logo_rejected(self):
        html = (
            '<html><head>'
            '<meta property="og:image" content="/themes/custom/site-logo.svg"/>'
            '</head><body></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) is None

    def test_empty_content_ignored(self):
        html = (
            '<html><head>'
            '<meta property="og:image" content="   "/>'
            '<meta name="twitter:image" content="https://cdn.example.com/tw.jpg"/>'
            '</head><body></body></html>'
        )
        assert extract_image_from_detail_html(html, self.BASE) == \
            "https://cdn.example.com/tw.jpg"


# ---------------------------------------------------------------------------
# fetch_detail_image — politeness, error handling, counter reuse
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, text: str):
        self.text = text


class _FakeScraper:
    def __init__(self, html: str | None = None, fail: bool = False):
        self._detail_image_fetches = 0
        self._html = html
        self._fail = fail
        self.calls: list[str] = []

    def get(self, url: str) -> _FakeResponse:
        self.calls.append(url)
        if self._fail:
            raise RuntimeError("network down")
        assert self._html is not None
        return _FakeResponse(self._html)


class TestFetchDetailImage:
    URL = "https://example.com/events/1"
    HTML = (
        '<html><head>'
        '<meta property="og:image" content="https://cdn.example.com/e.jpg"/>'
        '</head><body></body></html>'
    )

    def test_success(self):
        scraper = _FakeScraper(html=self.HTML)
        out = fetch_detail_image(scraper, self.URL)
        assert out == "https://cdn.example.com/e.jpg"
        assert scraper._detail_image_fetches == 1
        assert scraper.calls == [self.URL]

    def test_no_image_tag_returns_none(self):
        scraper = _FakeScraper(html="<html><head></head><body></body></html>")
        assert fetch_detail_image(scraper, self.URL) is None

    def test_fetch_failure_never_raises(self):
        scraper = _FakeScraper(fail=True)
        assert fetch_detail_image(scraper, self.URL) is None
        assert scraper._detail_image_fetches == 1

    def test_limit_respected(self):
        scraper = _FakeScraper(html=self.HTML)
        assert fetch_detail_image(scraper, self.URL, limit=2) is not None
        assert fetch_detail_image(scraper, self.URL, limit=2) is not None
        # Third call hits the cap: no request is made.
        assert fetch_detail_image(scraper, self.URL, limit=2) is None
        assert scraper.calls == [self.URL, self.URL]


# ---------------------------------------------------------------------------
# Integration smoke: scrapers set image_url on raw records when the detail
# page they already fetch carries an og:image (mocked at the HTTP layer).
# ---------------------------------------------------------------------------

class TestScraperIntegration:
    DETAIL = (
        '<html><head>'
        '<meta property="og:image" content="https://cdn.example.com/hike.jpg"/>'
        '</head><body><div class="event-cost">$25 per person</div></body></html>'
    )

    def test_arlington_parks_sets_image_url(self):
        from scrapers.arlington_parks import ArlingtonParksRecScraper

        scraper = ArlingtonParksRecScraper()
        card_html = """
        <div class="list-item-container">
          <h2 class="list-item-title">Nature Hike</h2>
          <a href="/Parks-Events/nature-hike">Details</a>
        </div>
        """
        from bs4 import BeautifulSoup

        card = BeautifulSoup(card_html, "lxml").select_one("div.list-item-container")
        scraper.get = lambda url: _FakeResponse(self.DETAIL)  # type: ignore[method-assign]
        raw = scraper._parse_card(card)
        assert raw is not None
        assert raw["image_url"] == "https://cdn.example.com/hike.jpg"
        # Pricing still extracted from the same fetched page (no double fetch).
        assert raw["price_text"] == "$25 per person"
