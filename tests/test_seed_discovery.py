"""
Tests for the DullesMoms seed-discovery layer.

These cover the policy-critical invariant — DullesMoms is a pointer, never a
source of record — and the two link-selection bugs that made the discovery
path publish nothing useful:

  1. The seed finder accepted the first outbound https link on an article,
     including the aggregator's own social links, so candidates resolved to
     facebook.com/DullesMoms with that page's bio as the event description.
  2. A title anchor with no href fell back to the calendar list URL, which the
     resolver then "detail-scraped", picking up the listing's own chrome.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from models.candidate import CandidateEvent
from seed_discovery.dullesmoms_seed_finder import SEED_URL, DullesMomsSeedFinder
from seed_discovery.resolver import _is_detail_page_url, resolve_candidate
from seed_discovery.urls import is_candidate_original_url, is_seed_url, is_utility_url


def _article(html: str) -> BeautifulSoup:
    """Parse an event article fragment the way the seed finder does."""
    return BeautifulSoup(html, "lxml").select_one("article")


# ---------------------------------------------------------------------------
# Shared URL predicates
# ---------------------------------------------------------------------------

class TestUrlPredicates:
    def test_seed_domain_is_never_an_original_url(self):
        assert is_seed_url("https://dullesmoms.com/dmcalendar/list/")
        assert is_seed_url("https://www.dullesmoms.com/event/story-time/")
        assert not is_candidate_original_url("https://dullesmoms.com/event/x/")

    def test_social_and_calendar_links_are_utility(self):
        for url in (
            "https://www.facebook.com/DullesMoms",
            "https://instagram.com/dullesmoms",
            "https://twitter.com/intent/tweet?url=x",
            "https://calendar.google.com/calendar/render?action=TEMPLATE",
            "https://outlook.live.com/owa/?path=/calendar/action/compose",
            "https://maps.google.com/?q=Reston",
        ):
            assert is_utility_url(url), url
            assert not is_candidate_original_url(url), url

    def test_real_event_hosts_are_accepted(self):
        for url in (
            "https://www.fairfaxcounty.gov/parks/events/spring-fair",
            "https://www.eventbrite.com/e/kids-workshop-tickets-123",
            "https://loudoun.gov/calendar/story-time",
            "https://www.lowes.com/l/kids-workshop.html",
        ):
            assert is_candidate_original_url(url), url

    def test_relative_and_malformed_urls_rejected(self):
        for url in ("", "/event/foo/", "#content", "mailto:x@y.com", "javascript:void(0)"):
            assert not is_candidate_original_url(url), url


# ---------------------------------------------------------------------------
# Bug 1 — social links must not become candidate original URLs
# ---------------------------------------------------------------------------

class TestOutboundLinkSelection:
    def test_social_link_is_not_chosen_as_original_url(self):
        """The exact shape that produced the facebook.com/DullesMoms candidates."""
        article = _article(
            """
            <article class="type-tribe_events">
              <h2 class="tribe-events-calendar-list__event-title">
                <a href="https://dullesmoms.com/event/touch-a-truck/">Touch-a-Truck</a>
              </h2>
              <a href="https://www.facebook.com/DullesMoms">Follow us on Facebook</a>
            </article>
            """
        )
        finder = DullesMomsSeedFinder.__new__(DullesMomsSeedFinder)
        assert finder._find_original_url(article, "https://dullesmoms.com/event/x/") is None

    def test_real_host_chosen_over_social_link(self):
        article = _article(
            """
            <article class="type-tribe_events">
              <a href="https://www.facebook.com/DullesMoms">Share</a>
              <a href="https://www.restontowncenter.com/touch-a-truck">Event Website</a>
            </article>
            """
        )
        finder = DullesMomsSeedFinder.__new__(DullesMomsSeedFinder)
        assert (
            finder._find_original_url(article, "https://dullesmoms.com/event/x/")
            == "https://www.restontowncenter.com/touch-a-truck"
        )

    def test_priority_link_text_wins_over_document_order(self):
        article = _article(
            """
            <article class="type-tribe_events">
              <a href="https://sponsor.example.com/ad">Our sponsor</a>
              <a href="https://www.fairfaxcounty.gov/parks/e/1">Register</a>
            </article>
            """
        )
        finder = DullesMomsSeedFinder.__new__(DullesMomsSeedFinder)
        assert (
            finder._find_original_url(article, "https://dullesmoms.com/event/x/")
            == "https://www.fairfaxcounty.gov/parks/e/1"
        )


# ---------------------------------------------------------------------------
# Bug 2 — the list page must never be treated as a per-event detail page
# ---------------------------------------------------------------------------

class TestDetailUrlResolution:
    def test_missing_href_yields_no_detail_url(self):
        assert DullesMomsSeedFinder._detail_url("") is None
        assert DullesMomsSeedFinder._detail_url("#") is None

    def test_list_page_href_yields_no_detail_url(self):
        assert DullesMomsSeedFinder._detail_url(SEED_URL) is None
        assert DullesMomsSeedFinder._detail_url("/dmcalendar/list/") is None

    def test_relative_event_href_is_made_absolute(self):
        assert (
            DullesMomsSeedFinder._detail_url("/event/story-time/")
            == "https://dullesmoms.com/event/story-time/"
        )

    def test_offsite_href_is_not_a_detail_url(self):
        assert DullesMomsSeedFinder._detail_url("https://example.com/e/1") is None

    def test_resolver_rejects_listing_urls_as_detail_pages(self):
        for url in (
            SEED_URL,
            "https://dullesmoms.com/dmcalendar/",
            "https://dullesmoms.com/dmcalendar/month/",
            "https://dullesmoms.com/dmcalendar/today",
        ):
            assert not _is_detail_page_url(url), url

    def test_resolver_accepts_per_event_urls_as_detail_pages(self):
        assert _is_detail_page_url("https://dullesmoms.com/event/story-time/")
        assert _is_detail_page_url("https://dullesmoms.com/dmcalendar/list/story-time/")

    def test_non_seed_urls_are_not_detail_pages(self):
        assert not _is_detail_page_url("https://example.com/event/1")
        assert not _is_detail_page_url("")


# ---------------------------------------------------------------------------
# Candidate construction end-to-end
# ---------------------------------------------------------------------------

class TestCandidateParsing:
    def test_article_without_detail_link_routes_to_review(self):
        article = _article(
            """
            <article class="type-tribe_events">
              <h2 class="tribe-events-calendar-list__event-title"><a>Mystery Event</a></h2>
              <a href="https://www.facebook.com/DullesMoms">Facebook</a>
            </article>
            """
        )
        finder = DullesMomsSeedFinder.__new__(DullesMomsSeedFinder)
        finder.target_week_start = None
        candidate = finder._parse_article(article)

        assert candidate is not None
        assert candidate.candidate_original_url is None
        assert candidate.requires_manual_review is True
        assert "no DullesMoms detail page" in (candidate.notes or "")

    def test_well_formed_article_produces_resolvable_candidate(self):
        article = _article(
            """
            <article class="type-tribe_events">
              <h2 class="tribe-events-calendar-list__event-title">
                <a href="/event/spring-fair/">Spring Fair</a>
              </h2>
              <abbr class="tribe-events-abbr" title="2026-04-11">April 11, 2026</abbr>
              <div class="tribe-venue">Frying Pan Park</div>
              <a href="https://www.fairfaxcounty.gov/parks/spring-fair">Event Website</a>
            </article>
            """
        )
        finder = DullesMomsSeedFinder.__new__(DullesMomsSeedFinder)
        finder.target_week_start = None
        candidate = finder._parse_article(article)

        assert candidate is not None
        assert candidate.seed_url == "https://dullesmoms.com/event/spring-fair/"
        assert (
            candidate.candidate_original_url
            == "https://www.fairfaxcounty.gov/parks/spring-fair"
        )
        assert candidate.requires_manual_review is False
        assert candidate.confidence >= 0.5
        # Discovered text is retained for matching only, never as content.
        assert candidate.discovered_title == "Spring Fair"
        assert candidate.discovered_location_text == "Frying Pan Park"


# ---------------------------------------------------------------------------
# Stale queue entries must not resolve to the aggregator's own pages
# ---------------------------------------------------------------------------

class TestStaleCandidateSanitisation:
    def test_queued_social_url_is_discarded_without_fetching(self):
        """
        Mirrors the 711 stale entries in pending_candidates.json: a social URL
        plus a seed_url pointing at the calendar listing.  Resolution must fail
        closed — no HTTP, no publish — rather than republishing the aggregator.
        """
        candidate = CandidateEvent(
            candidate_id="f3a8b1c2d4e56789",
            seed_source_name="DullesMoms",
            seed_url=SEED_URL,
            discovered_title="Touch-a-Truck Event at Reston Town Center",
            candidate_original_url="https://www.facebook.com/DullesMoms",
            confidence=0.47,
        )

        def _fail(*args, **kwargs):  # pragma: no cover - must never run
            raise AssertionError("resolver attempted a network fetch")

        class _NoNetworkSession:
            get = staticmethod(_fail)

        assert resolve_candidate(candidate, session=_NoNetworkSession()) is None
        assert candidate.candidate_original_url is None
        assert candidate.requires_manual_review is True
        assert candidate.review_reason == "no_original_url_found"
        assert "calendar listing" in (candidate.last_resolution_error or "")
