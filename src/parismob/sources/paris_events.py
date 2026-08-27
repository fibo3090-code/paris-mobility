"""Que Faire a Paris -- cultural events with dates and coordinates (ODbL).

Roughly 3,000 live records. Two things to be honest about:

* It is **culture-focused**. It does not reliably carry PSG fixtures or Stade de
  France concerts, which is exactly why the hand-curated venue registry exists
  separately rather than being derived from this feed.
* It is a **live** feed, not an archive. Records disappear once an event passes,
  which makes it perishable in the same way the real-time feeds are: today's
  snapshot cannot be reconstructed next month. It is therefore partitioned by
  snapshot date, and old snapshots are never overwritten.

Known dirt, handled below: null dates, and `lat_lon` of exactly {0.0, 0.0} used
as a null island placeholder.
"""

from __future__ import annotations

import datetime as dt
import json

import polars as pl

from ..lake import raw_path, record_fetch, sha256_bytes, write_partition
from .base import Written, client, ods_records

name = "paris_events"

BASE = "https://opendata.paris.fr"
DATASET = "que-faire-a-paris-"

def fetch(snapshot: str | None = None) -> list[Written]:
    """Take a dated snapshot of the live event feed."""
    day = snapshot or dt.date.today().isoformat()

    with client() as cli:
        records = ods_records(
            DATASET,
            base=BASE,
            select=(
                "id,url,title,lead_text,date_start,date_end,occurrences,date_description,"
                "address_name,address_street,address_zipcode,address_city,lat_lon,"
                "audience,price_type,access_type,updated_at,qfap_tags"
            ),
            cli=cli,
        )

    payload = json.dumps(records, ensure_ascii=False).encode("utf-8")
    dest = raw_path(name, f"que-faire-a-paris-{day}.json")
    dest.write_bytes(payload)
    record_fetch(
        source=name,
        url=f"{BASE}/api/explore/v2.1/catalog/datasets/{DATASET}/records",
        path=dest,
        n_bytes=len(payload),
        digest=sha256_bytes(payload),
        rows=len(records),
        note=f"event snapshot {day}",
    )

    rows = []
    for r in records:
        ll = r.get("lat_lon") or {}
        lat, lon = ll.get("lat"), ll.get("lon")
        # Null island: a placeholder, not a location off the coast of Africa.
        if lat == 0.0 and lon == 0.0:
            lat = lon = None
        tags = r.get("qfap_tags")
        rows.append(
            {
                "id": str(r.get("id")) if r.get("id") is not None else None,
                "title": r.get("title"),
                "url": r.get("url"),
                "lead_text": r.get("lead_text"),
                "date_start": r.get("date_start"),
                "date_end": r.get("date_end"),
                "occurrences": r.get("occurrences"),
                "date_description": r.get("date_description"),
                "address_name": r.get("address_name"),
                "address_street": r.get("address_street"),
                "address_zipcode": r.get("address_zipcode"),
                "address_city": r.get("address_city"),
                "lat": lat,
                "lon": lon,
                "audience": r.get("audience"),
                "price_type": r.get("price_type"),
                "access_type": r.get("access_type"),
                "updated_at": r.get("updated_at"),
                "tags": ", ".join(tags) if isinstance(tags, list) else tags,
            }
        )

    # Event dates arrive as plain `YYYY-MM-DD`, but the feed has historically mixed
    # in full timestamps. Try the date form first and fall back, rather than letting
    # polars infer -- inference across a mixed column silently nulls the minority.
    def as_date(col: str):
        return (
            pl.coalesce(
                pl.col(col).str.to_date("%Y-%m-%d", strict=False),
                pl.col(col).str.to_datetime("%Y-%m-%dT%H:%M:%S%z", strict=False)
                .dt.convert_time_zone("Europe/Paris")
                .dt.date(),
            )
        ).alias(col)

    df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        as_date("date_start"),
        as_date("date_end"),
        pl.col("lat").cast(pl.Float64, strict=False),
        pl.col("lon").cast(pl.Float64, strict=False),
        pl.lit(day).str.to_date().alias("snapshot_date"),
    )

    path = write_partition(df, name, day)
    no_date = int(df["date_start"].is_null().sum())
    no_geo = int(df["lat"].is_null().sum())
    notes = [f"{no_date} rows without a start date", f"{no_geo} rows without usable coordinates"]
    return [Written(name, day, df.height, path, notes)]
