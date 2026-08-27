"""Referentiel des arrets -- the chain that links a SIRI stop to a venue.

Three identifier levels, and you need all three to get from a real-time feed back
to the validations:

    StopPoint (`arrid`)  -- a single quay or platform. What SIRI addresses.
      -> ZDA (`zdaid`)   -- the stop zone grouping those quays.
        -> ZDC (`zdcid`) -- the interchange zone. What validations call
                            `ID_REFA_LDA` / `lda` / `ID_ZDC`, and what the venue
                            registry stores.

Verified end to end: `STIF:StopPoint:Q:478977:` in the real-time perimeter is
`arrid` 478977 in `arrets` ("Mairie de Vitry-sur-Seine"), whose `zdaid` resolves
through `zones-d-arrets` to a `zdcid` that joins the validations.

Both tables exceed the ODS 10,000-offset ceiling, so they come through the bulk
export endpoint rather than paged records.

Coordinates are Lambert-93 (EPSG:2154) in `arrxepsg2154` / `arryepsg2154`. They are
correctly labelled here -- unlike in the real-time perimeter file, where the X and
Y values sit under keys named Longitude and Latitude the wrong way round.
"""

from __future__ import annotations

import io

import polars as pl

from ..lake import raw_path, record_fetch, sha256_bytes, write_partition
from .base import Written, client, ods_export

name = "idfm_referentiel"

BASE = "https://data.iledefrance-mobilites.fr"


def _archive_and_parse(dataset: str, payload: bytes, note: str) -> pl.DataFrame:
    dest = raw_path(name, f"{dataset}.csv")
    dest.write_bytes(payload)
    df = pl.read_csv(
        io.BytesIO(payload),
        separator=";",
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )
    record_fetch(
        source=name,
        url=f"{BASE}/api/explore/v2.1/catalog/datasets/{dataset}/exports/csv",
        path=dest,
        n_bytes=len(payload),
        digest=sha256_bytes(payload),
        rows=df.height,
        note=note,
    )
    return df


def fetch(force: bool = False) -> list[Written]:
    out: list[Written] = []
    with client() as cli:
        arrets = _archive_and_parse(
            "arrets", ods_export("arrets", base=BASE, cli=cli), "stop points"
        )
        arrets = arrets.with_columns(
            pl.col("arrid").cast(pl.Int64, strict=False),
            pl.col("zdaid").cast(pl.Int64, strict=False),
            pl.col("arrxepsg2154").cast(pl.Float64, strict=False),
            pl.col("arryepsg2154").cast(pl.Float64, strict=False),
        )
        out.append(
            Written(
                "idfm_arrets",
                "current",
                arrets.height,
                write_partition(arrets, "idfm_arrets", "current"),
                [f"{arrets['zdaid'].n_unique()} ZDA distinctes"],
            )
        )

        zones = _archive_and_parse(
            "zones-d-arrets", ods_export("zones-d-arrets", base=BASE, cli=cli), "stop zones"
        )
        zones = zones.with_columns(
            pl.col("zdaid").cast(pl.Int64, strict=False),
            pl.col("zdcid").cast(pl.Int64, strict=False),
        )
        out.append(
            Written(
                "idfm_zones_arrets",
                "current",
                zones.height,
                write_partition(zones, "idfm_zones_arrets", "current"),
                [f"{zones['zdcid'].n_unique()} ZDC distinctes"],
            )
        )
    return out


def stoppoints_for_zdc(zdc: list[int]) -> pl.DataFrame:
    """Every StopPoint belonging to a set of ZDC ids, via the ZDA level."""
    from ..config import LAKE_DIR

    a = LAKE_DIR / "idfm_arrets" / "current.parquet"
    z = LAKE_DIR / "idfm_zones_arrets" / "current.parquet"
    if not a.exists() or not z.exists():
        raise FileNotFoundError("run `parismob fetch referentiel` first")

    zones = pl.read_parquet(z).filter(pl.col("zdcid").is_in(zdc)).select("zdaid", "zdcid", "zdaname")
    return (
        pl.read_parquet(a)
        .join(zones, on="zdaid", how="inner")
        .select("arrid", "arrname", "arrtype", "zdaid", "zdcid", "zdaname")
        .unique(subset=["arrid"])
        .sort("zdcid", "arrid")
    )
