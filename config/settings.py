"""
Pipeline-wide settings loaded from environment variables and config files.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Directory layout
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
NORMALIZED_DIR = DATA_DIR / "normalized"
PUBLISHED_DIR = DATA_DIR / "published" / "events"
MANUAL_REVIEW_DIR = DATA_DIR / "manual_review"
CONFIG_DIR = ROOT_DIR / "config"
SOURCES_FILE = CONFIG_DIR / "sources.yaml"

# Ensure runtime directories exist
for _d in (RAW_DIR, NORMALIZED_DIR, PUBLISHED_DIR, MANUAL_REVIEW_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# HTTP behaviour
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT: int = 20          # seconds per request
REQUEST_DELAY: float = 1.5         # polite delay between requests (seconds)
REQUEST_MAX_RETRIES: int = 3

USER_AGENT: str = (
    "NoVAKidsPipeline/1.0 (family activities aggregator; "
    "contact: pipeline@example.com)"
)

# ---------------------------------------------------------------------------
# Optional integrations
# ---------------------------------------------------------------------------

EVENTBRITE_API_KEY: str | None = os.environ.get("EVENTBRITE_API_KEY")

# ---------------------------------------------------------------------------
# Seed discovery
# ---------------------------------------------------------------------------

# Minimum combined confidence for a seed-resolved event to be auto-published.
# Candidates below this threshold go to data/manual_review/pending_candidates.json.
SEED_CONFIDENCE_THRESHOLD: float = 0.5

# When True, events resolved from seed discovery are included in the pipeline run.
SEED_DISCOVERY_ENABLED: bool = False  # opt-in via --with-seed-discovery flag

# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------

WEEK_START_DAY: int = 0  # 0 = Monday (ISO week start)
