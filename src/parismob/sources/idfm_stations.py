"""IDFM station reference -- the join key between venues and validations.

`emplacement-des-gares-idf` carries one row per station-and-line, with both a
coordinate and `id_ref_zdc`. That column is what the validations files call
`ID_REFA_LDA` / `lda` / `ID_ZDC`, which makes it the hinge of the whole model:
it is how "this venue is served by these stations" becomes a numeric join rather
than a string match.

Name matching is not an option here. The validations files uppercase and
abbreviate (`LA PLAINE-STADE DE FRANCE`), and a substring search for
`SAINT-DENIS` also returns `STRASBOURG-SAINT-DENIS`, a metro stop in central
Paris with no connection to the stadium. Venues resolve by distance instead.
"""

from __future__ import annotations

import json
import math

import polars as pl

from ..lake import raw_path, record_fetch, sha256_bytes, write_partition
from .base import Written, client, ods_records

name = "idfm_stations"

BASE = "https://data.iledefrance-mobilites.fr"
DATASET = "emplacement-des-gares-idf"

EARTH_RADIUS_M = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def fetch(force: bool = False) -> list[Written]:
    """Pull the full station reference and flatten the nested coordinate."""
    with client() as cli:
        records = ods_records(
            DATASET,
            base=BASE,
            select="nom_gares,nom_zdc,id_ref_zdc,id_ref_zda,res_com,mode,exploitant,geo_point_2d",
            cli=cli,
        )

    # The reference arrives as paged JSON rather than one downloadable file, so
    # the archive step writes the assembled payload -- same invariant, same manifest.
    payload = json.dumps(records, ensure_ascii=False).encode("utf-8")
    dest = raw_path(name, "emplacement-des-gares-idf.json")
    dest.write_bytes(payload)
    record_fetch(
        source=name,
        url=f"{BASE}/api/explore/v2.1/catalog/datasets/{DATASET}/records",
        path=dest,
        n_bytes=len(payload),
        digest=sha256_bytes(payload),
        rows=len(records),
        note="station reference (paged)",
    )

    rows = []
    for r in records:
        pt = r.get("geo_point_2d") or {}
        rows.append(
            {
                "nom_gares": r.get("nom_gares"),
                "nom_zdc": r.get("nom_zdc"),
                "id_ref_zdc": r.get("id_ref_zdc"),
                "id_ref_zda": r.get("id_ref_zda"),
                "res_com": r.get("res_com"),
                "mode": r.get("mode"),
                "exploitant": r.get("exploitant"),
                "lat": pt.get("lat"),
                "lon": pt.get("lon"),
            }
        )

    df = pl.DataFrame(rows).with_columns(
        pl.col("id_ref_zdc").cast(pl.Int64, strict=False),
        pl.col("id_ref_zda").cast(pl.Int64, strict=False),
        pl.col("lat").cast(pl.Float64, strict=False),
        pl.col("lon").cast(pl.Float64, strict=False),
    )
    path = write_partition(df, name, "current")
    return [Written(name, "current", df.height, path, [f"{df['id_ref_zdc'].n_unique()} distinct ZDC ids"])]


def stations_near(lat: float, lon: float, radius_m: float = 900.0) -> pl.DataFrame:
    """Stations within `radius_m` of a point, nearest first, one row per ZDC.

    Used to resolve a venue's serving stations reproducibly. The default radius is
    a walkable distance rather than a tuned one; venues state their own where the
    site is large enough that the centroid is misleading.
    """
    from ..config import LAKE_DIR

    part = LAKE_DIR / name / "current.parquet"
    if not part.exists():
        raise FileNotFoundError("run `parismob fetch stations` first")

    df = pl.read_parquet(part).filter(pl.col("lat").is_not_null())
    dist = [haversine_m(lat, lon, la, lo) for la, lo in zip(df["lat"], df["lon"])]
    df = df.with_columns(pl.Series("distance_m", dist).round(0))
    return (
        df.filter(pl.col("distance_m") <= radius_m)
        .sort("distance_m")
        .group_by("id_ref_zdc", maintain_order=True)
        .agg(
            pl.col("nom_zdc").first(),
            pl.col("distance_m").first(),
            pl.col("res_com").unique().sort().str.join(", ").alias("lignes"),
        )
    )
