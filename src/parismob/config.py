"""Paths and constants. Everything file-system related resolves through here."""

from __future__ import annotations

import os
from pathlib import Path

# Override with PARISMOB_DATA=/mnt/big/parismob when you move to the 32 GB box.
DATA_DIR = Path(os.environ.get("PARISMOB_DATA", Path(__file__).resolve().parents[2] / "data"))

RAW_DIR = DATA_DIR / "raw"        # Untouched bytes, exactly as the server sent them.
LAKE_DIR = DATA_DIR / "lake"      # Parsed Parquet, partitioned by source.
REFERENCE_DIR = DATA_DIR / "reference"  # Hand-curated CSVs that live in git.
DB_PATH = DATA_DIR / "lake.duckdb"

USER_AGENT = "parismob/0.1 (research; +https://github.com/)"
TIMEOUT = 120.0


def ensure_dirs() -> None:
    for d in (RAW_DIR, LAKE_DIR, REFERENCE_DIR):
        d.mkdir(parents=True, exist_ok=True)
