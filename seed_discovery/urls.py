"""
URL classification shared by the seed finder and the resolver.

Both stages need the same answer to one question: "is this outbound link a
plausible original event host, or is it noise?"  They used to answer it
differently — the resolver filtered social and calendar links while the seed
finder took the first outbound https URL it saw.  That mismatch is what put
candidates like `facebook.com/DullesMoms` into the review queue with the
aggregator's own Facebook bio extracted as the event description.

Keeping the predicates here means a domain added to the deny list takes effect
at both stages at once.
"""

from __future__ import annotations

from urllib.parse import urlparse

SEED_DOMAIN = "dullesmoms.com"

#: Domains that are never an original event host.  A link to one of these is
#: either the aggregator's own social presence, an "add to calendar" widget,
#: or a share button — none of which describe the event.
UTILITY_DOMAINS: frozenset[str] = frozenset([
    # Social / share targets
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com", "tiktok.com",
    "threads.net", "reddit.com", "whatsapp.com",
    # Calendar widgets
    "google.com", "calendar.google.com", "maps.google.com",
    "apple.com", "outlook.live.com", "outlook.office.com",
    "outlook.office365.com", "addtocalendar.com", "addevent.com",
    # Mail / contact
    "mailto", "gmail.com",
])


def _netloc(url: str) -> str:
    """Return the lowercased hostname of a URL, without a leading 'www.'."""
    netloc = urlparse(url).netloc.lower()
    return netloc[4:] if netloc.startswith("www.") else netloc


def is_seed_url(url: str) -> bool:
    """
    Return True if the URL belongs to the seed aggregator (dullesmoms.com).

    Unparseable URLs are treated as seed URLs so that a malformed href is
    skipped rather than published as an original source.
    """
    try:
        return SEED_DOMAIN in _netloc(url)
    except Exception:
        return True


def is_utility_url(url: str) -> bool:
    """
    Return True if the URL is a social, share, or calendar-widget link
    rather than a candidate original event host.
    """
    try:
        netloc = _netloc(url)
    except Exception:
        return False
    if not netloc:
        return False
    return any(
        netloc == domain or netloc.endswith("." + domain)
        for domain in UTILITY_DOMAINS
    )


def is_candidate_original_url(url: str) -> bool:
    """
    Return True if `url` could plausibly be an original event host page.

    Requires an absolute http(s) URL that is neither the seed aggregator nor a
    utility/social link.  This is the single predicate both the seed finder and
    the resolver use when choosing an outbound link.
    """
    if not url or not url.startswith(("http://", "https://")):
        return False
    return not is_seed_url(url) and not is_utility_url(url)
