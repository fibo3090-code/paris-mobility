"""French public holidays and school holidays -- the cheapest strong features.

Two separate feeds, one table each.

Public holidays come from Etalab as a flat `{date: label}` object per year.

School holidays come from the Ministry of Education. Paris is **Zone C**, but the
dataset covers every academie and every zone, so it is stored whole and filtered
at query time -- the surrounding zones matter as soon as you care about people
travelling *into* Ile-de-France. `population` distinguishes pupils from staff and
is often `-` for both.

The daily calendar this produces is what makes an event feature legible: a Tuesday
in the February break behaves nothing like a Tuesday in November, and IDFM's own
`CAT_JOUR` typology in the validations profiles encodes exactly that distinction.
"""

from __future__ import annotations

import datetime as dt
import json

import polars as pl

from ..lake import raw_path, record_fetch, sha256_bytes, write_partition
from .base import Written, client, download, ods_records

name = "calendar_fr"

HOLIDAYS_URL = "https://calendrier.api.gouv.fr/jours-feries/metropole/{year}.json"
SCHOOL_BASE = "https://data.education.gouv.fr"
SCHOOL_DATASET = "fr-en-calendrier-scolaire"


def _fetch_public_holidays(years: list[int], force: bool, cli) -> Written:
    rows = []
    for y in years:
        got = download(
            HOLIDAYS_URL.format(year=y),
            source=name,
            filename=f"jours-feries-{y}.json",
            force=force,
            note=f"public holidays {y}",
            cli=cli,
        )
        for date_str, label in json.loads(got.data).items():
            rows.append({"date": date_str, "nom": label, "annee": y})

    df = pl.DataFrame(rows).with_columns(
        pl.col("date").str.to_date("%Y-%m-%d", strict=False)
    )
    path = write_partition(df, "jours_feries", "all")
    return Written("jours_feries", "all", df.height, path, [f"{years[0]}-{years[-1]}"])


def _fetch_school_holidays(force: bool, cli) -> Written:
    """The school calendar exceeds the ODS offset ceiling, so it is sliced by zone."""
    zones = ["Zone A", "Zone B", "Zone C", "Corse"]
    records: list[dict] = []
    for z in zones:
        records.extend(
            ods_records(
                SCHOOL_DATASET,
                base=SCHOOL_BASE,
                select="description,population,start_date,end_date,location,zones,annee_scolaire",
                where=f'zones = "{z}"',
                cli=cli,
            )
        )

    payload = json.dumps(records, ensure_ascii=False).encode("utf-8")
    dest = raw_path(name, "calendrier-scolaire.json")
    dest.write_bytes(payload)
    record_fetch(
        source=name,
        url=f"{SCHOOL_BASE}/api/explore/v2.1/catalog/datasets/{SCHOOL_DATASET}/records",
        path=dest,
        n_bytes=len(payload),
        digest=sha256_bytes(payload),
        rows=len(records),
        note="school holidays, sliced by zone",
    )

    # Timestamps are offset-aware (`2017-10-20T22:00:00+00:00`) and land the evening
    # before, because the calendar is expressed in Paris local time. The format is
    # given explicitly: polars refuses to infer one when an offset is present, and
    # inferring here would be how a silent off-by-one day enters the lake.
    as_date = (
        lambda col: pl.col(col)
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%z", strict=False)
        .dt.convert_time_zone("Europe/Paris")
        .dt.date()
        .alias(col)
    )
    df = pl.DataFrame(records).with_columns(as_date("start_date"), as_date("end_date"))
    path = write_partition(df, "vacances_scolaires", "all")
    zone_c = df.filter(pl.col("zones") == "Zone C").height
    return Written(
        "vacances_scolaires", "all", df.height, path, [f"{zone_c} rows for Zone C (Paris)"]
    )


def fetch(force: bool = False, from_year: int = 2015, to_year: int | None = None) -> list[Written]:
    to_year = to_year or dt.date.today().year + 1
    years = list(range(from_year, to_year + 1))
    with client() as cli:
        return [
            _fetch_public_holidays(years, force, cli),
            _fetch_school_holidays(force, cli),
        ]
