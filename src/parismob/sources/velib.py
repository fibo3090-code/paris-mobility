"""Velib' temps reel -- perissable, et sans aucune cle.

Le plan soutient que l'avantage du projet tient aux donnees perissables, et en
deduit qu'il faut PRIM. La premiere moitie est juste, la seconde trop etroite :
l'occupation des stations Velib' s'evapore exactement de la meme facon, personne
ne l'archive, et elle ne demande ni compte ni inscription. Ce collecteur peut
tourner ce soir ; PRIM attend une cle.

1 519 stations, velos mecaniques et electriques comptes separement, plus les
bornettes libres. Un sondage toutes les cinq minutes represente 288 instantanes
par jour et environ 437 000 lignes -- quelques Mo de Parquet.

Chaque sondage est sa propre partition, horodatee en UTC, et rien ne reecrit une
partition anterieure : l'interet de l'archive est ce qui etait vrai a l'instant t,
y compris ce qui sera corrige plus tard.

Le champ `duedate` porte l'horodatage de la station elle-meme, distinct de l'heure
du sondage. Les deux sont conserves : leur ecart mesure la fraicheur reelle du
flux, qui n'est pas garantie uniforme entre stations.
"""

from __future__ import annotations

import datetime as dt
import json

import polars as pl

from ..lake import raw_path, record_fetch, sha256_bytes, write_partition
from .base import Written, client, ods_records

name = "velib"

BASE = "https://opendata.paris.fr"
DATASET = "velib-disponibilite-en-temps-reel"


def _stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch(force: bool = False) -> list[Written]:
    stamp = _stamp()
    with client() as cli:
        records = ods_records(DATASET, base=BASE, cli=cli)

    payload = json.dumps(records, ensure_ascii=False).encode("utf-8")
    dest = raw_path(name, f"velib-{stamp}.json")
    dest.write_bytes(payload)
    record_fetch(
        source=name,
        url=f"{BASE}/api/explore/v2.1/catalog/datasets/{DATASET}/records",
        path=dest,
        n_bytes=len(payload),
        digest=sha256_bytes(payload),
        rows=len(records),
        note=f"instantane velib {stamp}",
    )

    rows = []
    for r in records:
        geo = r.get("coordonnees_geo") or {}
        rows.append(
            {
                "polled_at": stamp,
                "stationcode": r.get("stationcode"),
                "name": r.get("name"),
                "capacity": r.get("capacity"),
                "numbikesavailable": r.get("numbikesavailable"),
                "mechanical": r.get("mechanical"),
                "ebike": r.get("ebike"),
                "numdocksavailable": r.get("numdocksavailable"),
                "is_installed": r.get("is_installed"),
                "is_renting": r.get("is_renting"),
                "is_returning": r.get("is_returning"),
                "duedate": r.get("duedate"),
                "commune": r.get("nom_arrondissement_communes"),
                "lat": geo.get("lat"),
                "lon": geo.get("lon"),
            }
        )

    df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.col(["capacity", "numbikesavailable", "mechanical", "ebike", "numdocksavailable"]).cast(
            pl.Int64, strict=False
        ),
        pl.col(["lat", "lon"]).cast(pl.Float64, strict=False),
        # Format donne explicitement : polars refuse d'inferer quand un decalage
        # horaire est present, et inferer ici serait le chemin par lequel un
        # decalage d'une heure entre dans le lac sans bruit.
        pl.col("duedate").str.to_datetime("%Y-%m-%dT%H:%M:%S%z", strict=False),
    )
    # Un vrai indicateur de saturation : une station pleine et une station vide sont
    # toutes deux inutilisables, dans des sens opposes.
    df = df.with_columns(
        (pl.col("numbikesavailable") / pl.col("capacity")).alias("taux_remplissage")
    )

    path = write_partition(df, name, stamp)
    vides = int((df["numbikesavailable"] == 0).sum())
    pleines = int((df["numdocksavailable"] == 0).sum())
    hs = int((df["is_renting"] != "OUI").sum())
    return [
        Written(
            name,
            stamp,
            df.height,
            path,
            [f"{vides} stations vides, {pleines} pleines, {hs} hors service"],
        )
    ]
