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
| Annual & Seasonal Attractions | County and state fairs, touring circuses, farm fall festivals, holiday light trails — via the curated source below |

All sources must be listed in `config/sources.yaml` with `enabled: true`
before the pipeline will use them.

---

## Curated Marquee Events

Some of the events families most want to see are structurally unscrapeable.
A county fair, a circus that pitches a tent for three weeks, a farm's
eight-week fall festival — these live on a single-purpose promotional site
that changes once a year, often JavaScript-rendered, usually with no
per-occurrence markup at all. Writing and maintaining a scraper per circus is
not viable; a human-verified entry refreshed annually is.

`config/curated_events.yaml` holds those entries, and
`scrapers/curated_events.py` expands each one into a raw record per open day.
The source performs **no HTTP**.

### Rules for curated entries

1. **`source_url` must be the event's own official page.** Never an
   aggregator, ticket reseller, news article, or listing site. Those may be
   used to *find* an event; they may not be cited as its source. This is the
   same rule that governs seed discovery — the aggregator is a pointer, not a
   source of record.
2. **Every field is transcribed from that official page.** Do not infer hours,
   pricing, or age ranges from a secondary write-up. Omit a field rather than
   guess. If the per-day schedule cannot be verified, leave the times off and
   say so in the summary rather than inventing a plausible-looking one.
3. **`verified_on` records when a human last read the official page.** Entries
   not re-verified within 180 days log a staleness warning on every run.
4. **Entries expire on their own.** Past occurrences, and anything beyond a
   120-day horizon, are dropped at scrape time. A file nobody has touched in a
   year publishes nothing rather than publishing last year's dates.
5. **Delete entries once their run is over** and will not repeat.
6. **Changes go through a PR**, like any other source change.

### What this source is not

It is not a general-purpose way to hand-enter events that a scraper could
cover. If an organization publishes a real calendar, write a scraper for it.
The curated file is for events whose publisher offers nothing to scrape.
Keeping it small is what keeps it accurate.

### Out-of-region entries

An entry may deliberately sit outside the service-area bounding box when it is
a genuine day-trip destination (the State Fair of Virginia in Doswell, for
example). Such entries publish with `is_mappable: false` and
`geo_within_service_region: false`, and appear in
`data/manual_review/suspicious_geocodes.json` under `--strict-region`. Note the
reason in a YAML comment on the entry so the geocode report is not mistaken
for a bug.

---

## DullesMoms — Discovery Layer Only

DullesMoms (`dullesmoms.com`) occupies a **special, limited role** in the pipeline.

### What DullesMoms IS allowed for

- **Seed discovery**: The `seed_discovery/dullesmoms_seed_finder.py` module
  may visit the DullesMoms calendar list page to identify candidate events and,
  where present, outbound links to original event host pages.

This discovery layer runs on every scheduled pipeline run via the
`--use-dullesmoms-seeds` flag in `.github/workflows/nova-kids-pipeline.yml`.
Enabling it does **not** relax any rule below: every event it contributes is
published with its original host's URL and content, and
`_check_no_dullesmoms_source_url` in `enrichment/validate.py` fails the run if a
`dullesmoms.com` URL ever reaches a published event.

### Choosing an outbound link

`seed_discovery/urls.py` holds the single predicate — `is_candidate_original_url`
— that both the seed finder and the resolver use to decide whether an outbound
link is a plausible original host. It rejects the seed domain itself along with
social, share, and calendar-widget domains.

Both stages must use it. When they disagreed, the finder accepted the first
outbound link on an article and candidates resolved to `facebook.com/DullesMoms`,
publishing the aggregator's own page bio as an event description. Add new
deny-list domains to `UTILITY_DOMAINS` so both stages pick them up at once.

Relatedly, a candidate's `seed_url` must be a **per-event detail page**, never
the calendar listing. The resolver refuses to detail-scrape a listing URL,
because the first outbound link on that page belongs to the site's own chrome
rather than to any event.

### What DullesMoms is NOT allowed for

- **Published content source**: DullesMoms descriptions, summaries, images,
  and calendar text must NEVER be stored as source-of-record content or
  published to the app.
- **source_name / source_url in published events**: A published event must
  never have `dullesmoms.com` in its `source_url` unless a manual override
  path is explicitly approved by the product owner. The validation layer will
  flag this as an error.
- **Formal data source in `sources.yaml`**: DullesMoms must not appear in the
  approved sources list.

### Workflow

```
DullesMoms calendar page
        ↓  (title, date text, location text, outbound URL only)
  CandidateEvent (NOT published)
        ↓  resolver.py fetches original host page
  Raw dict from original page
        ↓  normalize_record()
  Event (published with original source_url)
```

Candidates that cannot be resolved to an original URL are written to
`data/manual_review/pending_candidates.json` for human inspection.

### Prohibited Sources (community aggregators)

- **DullesMoms as a content source** — see the section above for the limited
  discovery-only role.
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

### Manual Review Queue

When seed discovery cannot resolve a candidate to an original host page, it
is written to `data/manual_review/pending_candidates.json` with:
- `requires_manual_review: true`
- A `notes` field explaining why it was flagged
- A `_review_instructions` field (informal) suggesting next steps

To approve a candidate manually:
1. Find the original event host URL yourself.
2. Set `candidate_original_url` to the confirmed URL.
3. Set `status` to `"manual_review_approved"`.
4. Run `scripts/run_pipeline.py` — manually approved candidates are treated
   as resolved with `extracted_from: "manual_review_approved"`.

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

### short_note

The `short_note` field is a single derived sentence based strictly on
structured facts extracted from the original source page.  Rules:
- Maximum 200 characters.
- Must be a single sentence.
- May only reference: venue name, cost type, activity type, age range,
  indoor/outdoor setting — and only when those facts were explicitly present
  in the original source.
- Must NOT reference DullesMoms content.
- Must NOT invent pricing, age ranges, or amenities.
- If facts are insufficient, leave `short_note: null`.

### Links

- `source_url` must always point to the **original source event page**, not
  to an aggregator or cached copy.
- `source_url` must never be a `dullesmoms.com` URL in published events.
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
