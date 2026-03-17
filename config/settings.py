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
CONFIG_DIR = ROOT_DIR / "config"
SOURCES_FILE = CONFIG_DIR / "sources.yaml"

# Ensure runtime directories exist
for _d in (RAW_DIR, NORMALIZED_DIR, PUBLISHED_DIR):
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
# Publishing
# ---------------------------------------------------------------------------

WEEK_START_DAY: int = 0  # 0 = Monday (ISO week start)
