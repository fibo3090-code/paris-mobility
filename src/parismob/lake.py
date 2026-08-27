"""Storage layer: raw archive, Parquet lake, and a manifest that records provenance.

Three rules this module exists to enforce.

1. Archive the raw bytes before parsing them. Your parser will be wrong at some
   point; when it is, you want to re-parse rather than re-download. Some of these
   feeds are also only published for a limited window.
2. Every fetch is recorded in a manifest: what, when, from where, how many bytes,
   and the sha256. Six months from now that is the difference between "I have data"
   and "I can show you where every row came from".
3. Writes are idempotent. Re-running a collector must not duplicate rows. We do
   this by writing one Parquet file per logical partition and replacing it whole.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl

from .config import DB_PATH, LAKE_DIR, RAW_DIR, ensure_dirs


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _manifest_path() -> Path:
    return RAW_DIR / "manifest.jsonl"


def record_fetch(
    source: str,
    url: str,
    path: Path,
    n_bytes: int,
    digest: str,
    rows: int | None = None,
    note: str = "",
) -> None:
    """Append one line to the manifest. Never rewrites history."""
    ensure_dirs()
    entry = {
        "fetched_at": utcnow(),
        "source": source,
        "url": url,
        "path": str(path),
        "bytes": n_bytes,
        "sha256": digest,
        "rows": rows,
        "note": note,
    }
    with _manifest_path().open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def manifest() -> pl.DataFrame:
    p = _manifest_path()
    if not p.exists():
        return pl.DataFrame()
    return pl.read_ndjson(p)


def already_have(digest: str) -> bool:
    """True if we have previously archived a payload with this exact content hash."""
    m = manifest()
    if m.is_empty() or "sha256" not in m.columns:
        return False
    return digest in set(m["sha256"].to_list())


def raw_path(source: str, filename: str) -> Path:
    d = RAW_DIR / source
    d.mkdir(parents=True, exist_ok=True)
    return d / filename


def write_partition(df: pl.DataFrame, source: str, partition: str) -> Path:
    """Write one logical partition, replacing it entirely.

    Replacing the whole partition is what makes re-runs idempotent: there is no
    append path, so there is no way to double-count.
    """
    d = LAKE_DIR / source
    d.mkdir(parents=True, exist_ok=True)
    out = d / f"{partition}.parquet"
    df.write_parquet(out, compression="zstd")
    return out


def connect() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with every Parquet partition exposed as a view.

    DuckDB reads Parquet directly off disk, so nothing is loaded into RAM until a
    query actually needs it. This is why the whole lake stays usable on a laptop.
    """
    ensure_dirs()
    con = duckdb.connect(str(DB_PATH))
    for src_dir in sorted(LAKE_DIR.iterdir()) if LAKE_DIR.exists() else []:
        if not src_dir.is_dir():
            continue
        if not any(src_dir.glob("*.parquet")):
            continue
        glob = str(src_dir / "*.parquet").replace("'", "''")
        con.execute(
            f"CREATE OR REPLACE VIEW {src_dir.name} AS "
            f"SELECT * FROM read_parquet('{glob}', union_by_name = true)"
        )
    return con


def lake_status() -> list[dict]:
    """One row per source: partitions, rows, on-disk size."""
    rows: list[dict] = []
    if not LAKE_DIR.exists():
        return rows
    for src_dir in sorted(LAKE_DIR.iterdir()):
        if not src_dir.is_dir():
            continue
        files = sorted(src_dir.glob("*.parquet"))
        if not files:
            continue
        n = sum(pl.read_parquet_schema(f) is not None and pl.scan_parquet(f).select(pl.len()).collect().item() for f in files)
        rows.append(
            {
                "source": src_dir.name,
                "partitions": len(files),
                "rows": n,
                "mb": round(sum(f.stat().st_size for f in files) / 1e6, 1),
            }
        )
    return rows
