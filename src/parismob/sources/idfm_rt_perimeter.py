"""Perimetre des donnees temps reel -- which stops PRIM actually covers.

The one dataset in this project that cannot be fetched programmatically. It sits
under "Licence Mobilite" on the IDFM portal: `/records` returns `ForbiddenAccess`
and `/exports/csv` returns the header row and nothing else. It has to be
downloaded by hand from the dataset page and imported with
`parismob fetch rt-perimeter --file <path>`.

It earns that friction. It answers two questions nothing else does:

* **Which stops have real-time data at all.** 34,492 of them, across 1,829 lines.
  Polling a stop outside this set returns nothing, quietly.
* **The exact `MonitoringRef` syntax SIRI expects** -- `STIF:StopPoint:Q:478977:`.
  Not `STIF:StopArea:SP:<id>:`, which is what this project assumed before the file
  was in hand, and which would have failed against every stop.

Coordinate trap
---------------
`ns2_location` is EPSG:2154 (Lambert-93), and its keys are **swapped**: the value
under `ns2:Longitude` is the Lambert northing and the one under `ns2:Latitude` is
the easting. Cross-checked against `arrets`, where stop 478977 is
`arrxepsg2154=655096, arryepsg2154=6854589` -- the mirror of what this file
labels. The columns are stored here under honest names and left in Lambert-93;
nothing needs them reprojected, because the join runs on identifiers.
"""

from __future__ import annotations

import io
import json
import re
from pathlib import Path

import polars as pl

from ..lake import raw_path, record_fetch, sha256_bytes, write_partition
from .base import Written

name = "idfm_rt_perimeter"

DATASET_PAGE = (
    "https://data.iledefrance-mobilites.fr/explore/dataset/"
    "perimetre-des-donnees-tr-disponibles-plateforme-idfm/export/"
)

#: `STIF:StopPoint:Q:478977:` -> 478977
_STOPPOINT_ID = re.compile(r"StopPoint:Q:(\d+)")

DOWNLOAD_HINT = (
    f"This dataset is gated (Licence Mobilite) and cannot be fetched by API.\n"
    f"Download the CSV by hand from:\n  {DATASET_PAGE}\n"
    f"then run:  parismob fetch rt-perimeter --file <path-to-csv>"
)


def _lambert(raw: str | None) -> tuple[float | None, float | None]:
    """Return (x, y) in EPSG:2154, undoing the swapped key names."""
    if not raw:
        return None, None
    try:
        loc = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None, None
    # Deliberately crossed: the file's "Longitude" holds the northing.
    y = loc.get("ns2:Longitude")
    x = loc.get("ns2:Latitude")
    try:
        return (float(x) if x is not None else None, float(y) if y is not None else None)
    except (TypeError, ValueError):
        return None, None


def fetch(file: str | None = None, force: bool = False) -> list[Written]:
    if not file:
        raise SystemExit(DOWNLOAD_HINT)
    src = Path(file).expanduser()
    if not src.exists():
        raise SystemExit(f"{src} not found.\n\n{DOWNLOAD_HINT}")

    payload = src.read_bytes()
    dest = raw_path(name, "perimetre-tr.csv")
    dest.write_bytes(payload)
    record_fetch(
        source=name,
        url=DATASET_PAGE,
        path=dest,
        n_bytes=len(payload),
        digest=sha256_bytes(payload),
        note=f"manual import from {src.name}",
    )

    # utf-8-sig: the portal's export carries a BOM.
    text = payload.decode("utf-8-sig", errors="replace")
    df = pl.read_csv(
        io.BytesIO(text.encode("utf-8")),
        separator=";",
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )

    expected = {"line", "name_line", "ns2_stoppointref", "ns2_stopname", "operatorname"}
    missing = expected - set(df.columns)
    if missing:
        raise SystemExit(
            f"{src.name} is missing {sorted(missing)}. Got {df.columns}.\n"
            f"Is this the right dataset?\n\n{DOWNLOAD_HINT}"
        )

    coords = [_lambert(v) for v in df["ns2_location"]] if "ns2_location" in df.columns else []
    df = df.with_columns(
        pl.col("ns2_stoppointref")
        .str.extract(_STOPPOINT_ID.pattern, 1)
        .cast(pl.Int64, strict=False)
        .alias("arrid"),
        pl.col("line").str.extract(r"Line::([^:]+):", 1).alias("line_id"),
    )
    if coords:
        df = df.with_columns(
            pl.Series("x_epsg2154", [c[0] for c in coords], dtype=pl.Float64),
            pl.Series("y_epsg2154", [c[1] for c in coords], dtype=pl.Float64),
        )

    df = df.rename({"ns2_stoppointref": "monitoring_ref", "ns2_stopname": "stop_name"})
    keep = (
        "monitoring_ref",
        "arrid",
        "stop_name",
        "line_id",
        "name_line",
        "operatorname",
        "x_epsg2154",
        "y_epsg2154",
    )
    df = df.select([c for c in keep if c in df.columns])

    unresolved = int(df["arrid"].is_null().sum())
    if unresolved > df.height * 0.01:
        raise SystemExit(
            f"{unresolved:,} of {df.height:,} stop references did not match "
            f"`StopPoint:Q:<id>`. The reference format has changed."
        )

    path = write_partition(df, name, "current")
    notes = [
        f"{df['monitoring_ref'].n_unique():,} arrets en temps reel",
        f"{df['line_id'].n_unique():,} lignes",
    ]
    if unresolved:
        notes.append(f"{unresolved} references non resolues")
    return [Written(name, "current", df.height, path, notes)]
