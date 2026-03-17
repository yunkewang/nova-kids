# Data Source Rules and Policy

This document defines the rules governing which event sources the NoVA Kids
pipeline may use, and how they must be treated.

---

## Approved Source Categories

The pipeline may only ingest events from **original, public sources** in the
following categories:

| Category | Examples |
|---|---|
| County Parks & Recreation | Fairfax County Park Authority, Arlington County Parks |
| Public Libraries | Fairfax County Public Library (FCPL), Arlington Public Library (APL) |
| Museums & Nature Centers | National Zoo, Udvar-Hazy Center, Potomac Overlook Regional Park |
| Government Cultural Programs | Fairfax County Arts, Arlington Cultural Affairs |
| Public Event Platforms | Eventbrite (family-friendly query, Northern Virginia) |

All sources must be listed in `config/sources.yaml` with `enabled: true`
before the pipeline will use them.

---

## Prohibited Sources

The following are **explicitly prohibited** as formal data sources:

- **DullesMoms** (dullesMoms.com) — This site is a community aggregator, not
  an original event publisher. Events on DullesMoms are typically sourced from
  the same venues already in our approved list. Using it as a scraping target
  would duplicate effort, risk copyright concerns, and produce data of lower
  provenance. Do not add it to `sources.yaml`.

- **Other community aggregators** — sites like NoVAParents, local Facebook
  groups, Nextdoor, or similar community-curated lists. These are secondary
  sources, not original publishers.

- **Paid/gated databases** — any source that requires a subscription or
  account to access event data.

---

## Source Quality Requirements

Before adding a new source, confirm:

1. **The source is an original publisher.** The organization runs the events
   themselves, or is the primary ticketing/registration platform.

2. **Public access.** The event listings are accessible without authentication
   or a paid subscription.

3. **Robots.txt compliance.** The pipeline must respect the source's
   `robots.txt`. If scraping is disallowed, use only an official API or
   RSS/iCal feed if available.

4. **Polite crawling.** Enforce the `REQUEST_DELAY` setting in
   `config/settings.py`. Do not hammer servers.

5. **Family relevance.** The source primarily publishes content relevant to
   families with children in Northern Virginia.

---

## Content Rules

### Summaries

Summaries in the `summary` field must be:

- **Derived from the source.** Copy a short excerpt or paraphrase the source
  description. Do not rewrite or significantly alter the meaning.
- **Non-fabricated.** If no summary is available from the source, leave
  `summary: null`. Do not generate fictional descriptions.
- **Brief.** Maximum 500 characters.

### Tags and Scores

- `tags` and `family_friendly_score` are **derived metadata** computed by the
  enrichment layer. They are never copied from the source.
- Tags must come from `ALLOWED_TAGS` in `config/schema.py`.
- Scores are algorithmic estimates, not editorial judgements.

### Links

- `source_url` must always point to the **original source event page**, not
  to an aggregator or cached copy.
- `registration_url` should point to the official registration form when
  available. Do not link to third-party resellers unless they are the
  authorized ticketing platform for that event.

---

## Maintenance Responsibilities

- **Source selectors** (CSS selectors in scraper files) must be verified
  against the live page structure before each pipeline run in production.
  Sites change their HTML; selectors will break.

- **Dead events.** If `last_verified_at` is more than 14 days old and the
  event's start date has passed, the event should be dropped from future
  published files.

- **Source additions.** Any new source added to `sources.yaml` must be
  reviewed for compliance with these rules before `enabled` is set to `true`.
  Changes to `sources.yaml` should go through a PR, not be committed directly
  to `main`.

---

## Summary

> The NoVA Kids pipeline is designed to surface **original public events**
> for Northern Virginia families, collected directly from the organizations
> that host them. It relies on original event links, derives metadata from
> source content rather than rewriting it, and never uses community
> aggregators (including DullesMoms) as formal data sources.
