# NoVA Kids — Family Activities Data Pipeline

A maintainable Python data pipeline that collects, normalizes, deduplicates,
enriches, and publishes weekly JSON event files for a Northern Virginia family
activities iOS app.

---

## Table of Contents

1. [Overview](#overview)
2. [Repository Layout](#repository-layout)
3. [Quick Start](#quick-start)
4. [Running the Pipeline Locally](#running-the-pipeline-locally)
5. [Seed Discovery (DullesMoms)](#seed-discovery-dullesmoms)
6. [Running Seed Discovery Locally](#running-seed-discovery-locally)
7. [CLI Scripts](#cli-scripts)
6. [Adding a New Source](#adding-a-new-source)
7. [Weekly Publish Format](#weekly-publish-format)
8. [Git Workflow](#git-workflow)
9. [Using Claude Code to Maintain This Repo](#using-claude-code-to-maintain-this-repo)

---

## Overview

The pipeline:

```
Scrapers → raw JSON → Normalize → Enrich → Dedupe → Validate → Publish
```

- **Sources**: county parks & rec, public libraries, Eventbrite (optional).
  See [docs/source_rules.md](docs/source_rules.md) for the full source policy.
- **Schema**: all events are validated against a single Pydantic model.
  See [docs/schema.md](docs/schema.md).
- **Output**: `data/published/events/week-YYYY-MM-DD.json` + `index.json`.

---

## Overview

The pipeline has two modes:

**Direct scraping** (always on):
```
Approved scrapers → raw JSON → Normalize → Enrich → Dedupe → Validate → Publish
```

**Seed discovery** (opt-in):
```
DullesMoms calendar (discovery only)
        ↓ titles, dates, outbound links — NO descriptions stored
  CandidateEvent objects (internal, never published)
        ↓ resolver fetches original host page
  Raw dict from original page
        ↓ normalize / enrich / publish (source_url = original host)
  Published event — OR — data/manual_review/pending_candidates.json
```

- DullesMoms is **discovery-only** — never a content source.
- All published events have `source_url` pointing to the original host.
- Unresolved candidates go to a manual review queue for human inspection.
- `short_note` is a single derived sentence from verified facts — never invented.

See [docs/source_rules.md](docs/source_rules.md) for the full source policy.

## Repository Layout

```
nova-kids/
├── models/
│   ├── __init__.py
│   └── candidate.py       # CandidateEvent model (internal, not published)
├── seed_discovery/
│   ├── __init__.py
│   ├── base.py            # BaseSeedFinder ABC
│   ├── dullesmoms_seed_finder.py  # discovery-only: titles + outbound URLs
│   └── resolver.py        # fetches original pages, extracts structured facts
├── config/
│   ├── __init__.py
│   ├── schema.py          # Pydantic Event model + ALLOWED_TAGS
│   ├── settings.py        # paths, HTTP settings, env vars
│   └── sources.yaml       # approved data sources
├── scrapers/
│   ├── __init__.py
│   ├── base.py            # BaseScraper abstract class
│   ├── registry.py        # source_id → scraper class map
│   ├── fairfax_parks.py
│   ├── arlington_parks.py
│   ├── fairfax_library.py
│   └── arlington_library.py
├── enrichment/
│   ├── __init__.py
│   ├── normalize.py       # raw dict → Event
│   ├── enrich.py          # tag/score derivation
│   ├── annotate.py        # short_note generation (fact-only, template-driven)
│   ├── dedupe.py          # deduplication logic
│   ├── validate.py        # validation rules + report
│   └── publish.py         # weekly JSON writer
├── scripts/
│   ├── run_pipeline.py         # main pipeline runner
│   ├── run_seed_discovery.py   # seed discovery + original page resolution
│   ├── validate_events.py      # standalone validator
│   └── dedupe_events.py        # standalone dedup tool
├── data/
│   ├── raw/               # raw scraper output (gitignored except .gitkeep)
│   ├── normalized/        # normalized events (gitignored except .gitkeep)
│   ├── manual_review/     # pending_candidates.json for human inspection
│   └── published/
│       └── events/        # published weekly JSON + index.json
├── docs/
│   ├── schema.md
│   └── source_rules.md
├── requirements.txt
└── README.md
```

---

## Quick Start

```bash
# Clone the repo
git clone https://github.com/your-org/nova-kids.git
cd nova-kids

# Create and activate a virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
python scripts/run_pipeline.py

# Dry run (no files written to published/)
python scripts/run_pipeline.py --dry-run

# Run a single source
python scripts/run_pipeline.py --source fairfax_park_authority

# Validate an existing events file
python scripts/validate_events.py data/published/events/week-2025-06-02.json
```

---

## Running the Pipeline Locally

### Prerequisites

- Python 3.11+
- `pip install -r requirements.txt`
- (Optional) `EVENTBRITE_API_KEY` environment variable for Eventbrite source

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `EVENTBRITE_API_KEY` | No | Enables the Eventbrite source |

### What happens on each run

1. Loads sources from `config/sources.yaml` (only `enabled: true` entries)
2. Runs each scraper → saves raw JSON to `data/raw/<source_id>.json`
3. Normalizes raw records → saves to `data/normalized/events.json`
4. Enriches events (tags, scores, rainy_day_friendly)
5. Deduplicates (exact-ID and cross-source fingerprint)
6. Validates — **halts with exit code 1 on any errors**
7. Publishes weekly JSON file + updates `index.json`
8. Prints summary report

---

## CLI Scripts

### `scripts/run_pipeline.py`

```
usage: run_pipeline.py [-h] [--source SOURCE_ID] [--with-seed-discovery]
                       [--dry-run] [--verbose]

Options:
  --source SOURCE_ID       Run only this source (repeat for multiple)
  --with-seed-discovery    Include seed-resolved events from seed_events.json
  --dry-run                Skip writing published files
  --verbose, -v            Enable DEBUG logging
```

### `scripts/run_seed_discovery.py`

```
usage: run_seed_discovery.py [-h] [--dry-run] [--no-resolve] [--verbose]

Options:
  --dry-run       Discover and resolve but do not write output files
  --no-resolve    Only run seed finder; skip fetching original pages
  --verbose, -v   Enable DEBUG logging
```

### `scripts/validate_events.py`

```
usage: validate_events.py [-h] [--verbose] [file]

Validates a JSON file containing a list of events.
Exits 1 if validation errors are found.
```

### `scripts/dedupe_events.py`

```
usage: dedupe_events.py [-h] [--output OUTPUT] [--verbose] [input]

Deduplicates events in a JSON file and writes the result.
```

---

## Seed Discovery (DullesMoms)

DullesMoms is a community aggregator for NoVA family events.  The pipeline
uses it **only as a discovery layer** to surface candidate events and outbound
links to original host pages.

**What the seed finder does:**
- Visits `https://dullesmoms.com/dmcalendar/list/`
- Extracts: event title, date text, location text, any outbound (non-DullesMoms) URL
- Does NOT store: descriptions, summaries, body text, or images

**What the resolver does:**
- Fetches each `candidate_original_url` (the actual venue/org/Eventbrite page)
- Extracts facts via schema.org JSON-LD → Open Graph → HTML patterns
- Returns a raw dict ready for `normalize_record()`
- Routes low-confidence events to `data/manual_review/pending_candidates.json`

**What is published:**
- `source_url` = the original host URL (never dullesmoms.com)
- `source_name` = the original host name
- `extracted_from` = `"seed_resolved"` or `"manual_review_approved"`
- No DullesMoms text appears anywhere in published JSON

---

## Running Seed Discovery Locally

```bash
# Step 1: Discover candidates + resolve original pages
python scripts/run_seed_discovery.py

# Verbose mode (see each fetch)
python scripts/run_seed_discovery.py --verbose

# Discovery only (no HTTP fetches to original pages)
python scripts/run_seed_discovery.py --no-resolve

# Step 2: Review the manual review queue
cat data/manual_review/pending_candidates.json
# For each pending candidate, find the original URL and update the record:
#   "candidate_original_url": "https://...",
#   "status": "manual_review_approved"

# Step 3: Run the full pipeline including seed-resolved events
python scripts/run_pipeline.py --with-seed-discovery --dry-run
# If output looks good:
python scripts/run_pipeline.py --with-seed-discovery

# Step 4: Commit and open a PR
git add data/published/ data/manual_review/
git commit -m "data: publish week-YYYY-MM-DD with seed-resolved events"
git push -u origin data/week-YYYY-MM-DD
gh pr create --title "Publish week-YYYY-MM-DD" --body "Weekly update including seed-resolved events."
```

### Understanding the manual review file

`data/manual_review/pending_candidates.json` contains events the pipeline
could not automatically resolve.  Each record has:

| Field | Meaning |
|---|---|
| `discovered_title` | Title as it appeared on DullesMoms |
| `candidate_original_url` | Best outbound URL found (may be null) |
| `confidence` | 0–1 score; below 0.5 means manual review needed |
| `notes` | Why it was flagged |
| `_review_instructions` | Informal suggestion for the reviewer |
| `status` | Set to `"manual_review_approved"` when confirmed |

The pipeline re-runs are safe: the review queue is merged, not overwritten,
so manual annotations are preserved across runs.

---

## Adding a New Source

1. Add an entry to `config/sources.yaml` with a unique `id`.
2. Create `scrapers/<source_id>.py` with a class subclassing `BaseScraper`.
   - Set `source_id` and `source_name` class attributes.
   - Implement `fetch_raw() -> list[dict]`.
3. Register it in `scrapers/registry.py`.
4. Test with `python scripts/run_pipeline.py --source <source_id> --dry-run`.

---

## Weekly Publish Format

### `data/published/events/week-YYYY-MM-DD.json`

```json
{
  "week_start": "2025-06-02",
  "generated_at": "2025-06-03T02:00:00+00:00",
  "source_count": 3,
  "event_count": 42,
  "events": [ ... ]
}
```

### `data/published/events/index.json`

```json
{
  "version": "1",
  "generated_at": "2025-06-03T02:00:00+00:00",
  "available_weeks": ["2025-05-26", "2025-06-02"],
  "latest_week": "2025-06-02"
}
```

The iOS app fetches `index.json` first to discover available weeks, then
fetches the specific weekly file it needs.

---

## Git Workflow

### Recommended weekly update cycle

```bash
# 1. Start from main
git checkout main
git pull origin main

# 2. Create a branch for this week's data update
git checkout -b data/week-2025-06-02

# 3. Run the pipeline
python scripts/run_pipeline.py

# 4. Review the diff
git diff data/published/

# 5. Commit only the published data
git add data/published/
git commit -m "data: publish week-2025-06-02 (42 events, 3 sources)"

# 6. Push and open a PR
git push -u origin data/week-2025-06-02
gh pr create --title "Publish week-2025-06-02" --body "Weekly event update."
```

Only merge after reviewing the diff. The iOS app does not consume `main`
directly — it reads the published JSON files, so bad data can be caught
and fixed before merging.

---

## Using Claude Code to Maintain This Repo

Claude Code can be used to run and update this pipeline safely via pull
requests. The recommended workflow is:

### Weekly data refresh

Ask Claude Code:
> "Run the pipeline in dry-run mode, review the output, then commit and open
> a PR for the published JSON if everything looks good."

Claude Code will:
1. Run `python scripts/run_pipeline.py --dry-run` and report the summary.
2. If you approve, run without `--dry-run` to write the files.
3. Create a branch (`data/week-YYYY-MM-DD`), commit only `data/published/`,
   and open a pull request for your review.

### Adding a new source

Ask Claude Code:
> "Add a scraper for [source name] at [URL] and open a PR."

Claude Code will:
1. Read `docs/source_rules.md` to verify the source is approved.
2. Create the scraper, register it, and add it to `sources.yaml` as disabled.
3. Run the pipeline with `--source <id> --dry-run` to test.
4. Open a PR for your review and enable decision.

### Fixing a broken scraper

Ask Claude Code:
> "The fairfax_parks scraper returned 0 events last run. Please investigate
> and fix it."

Claude Code will:
1. Inspect `data/raw/fairfax_park_authority.json` for clues.
2. Fetch the source URL and compare the HTML against the scraper selectors.
3. Update the selectors, test with `--dry-run`, and open a PR.

### Running seed discovery and review

Ask Claude Code:
> "Run seed discovery against DullesMoms, review the manual review queue,
> and open a PR for any events that resolved cleanly."

Claude Code will:
1. Run `python scripts/run_seed_discovery.py --dry-run` first.
2. Report the discovery summary (how many candidates, how many need review).
3. If you approve, run without `--dry-run`.
4. Summarize what is in `pending_candidates.json` for you to review.
5. Run `python scripts/run_pipeline.py --with-seed-discovery --dry-run`.
6. Open a PR with both `data/published/` and `data/manual_review/` changes.

Claude Code will **not** approve manual review candidates on your behalf —
that decision stays with you.

### Safety rules Claude Code follows

- Never pushes directly to `main`.
- Always opens a PR with a clear description.
- Never adds unapproved sources (see `docs/source_rules.md`).
- Never fabricates event data; only uses content fetched from original sources.
- Never stores or publishes DullesMoms descriptions, summaries, or images.
- Never sets `source_url` to a dullesmoms.com URL in published events.
- Commits only files under `data/published/` and `data/manual_review/` for data updates.
- Always runs `--dry-run` first and reports before writing files.
