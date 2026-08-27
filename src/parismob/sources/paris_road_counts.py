"""Comptages routiers permanents de Paris -- la contrepartie routiere des validations.

Debit horaire (`q`, vehicules/heure) et taux d'occupation (`k`) par troncon, issus
des capteurs permanents de la Ville de Paris. C'est la source qui permet de traiter
la route comme le rail est deja traite ici.

Deux jeux, et la distinction compte
-----------------------------------
* `comptages-routiers-permanents` -- le flux courant. **Fenetre glissante de
  ~14 mois** (verifie : 2025-07-01 a 2026-08-26, 28,9 M lignes). Ce qui en sort
  n'est pas archive ailleurs par la Ville : c'est perissable, au meme titre que
  PRIM, et sans aucune cle.
* `comptages-routiers-permanents-historique` -- 15 ZIP annuels, 2010 a 2024,
  attaches au jeu et non exposes en enregistrements. Meme motif que les
  validations IDFM : `/records` renvoie zero, tout est dans `/attachments`.

Le plan affirmait « 3 000+ troncons, horaire, depuis 2010 ». C'est vrai de
l'archive, pas du flux courant -- les deux doivent etre collectes.

Volume
------
Une annee fait ~186 Mo compresses et ~7 Go de texte, en fichiers hebdomadaires.
L'essentiel est la colonne `dessin`, qui repete la geometrie complete du troncon
**sur chaque ligne horaire**. Elle est retiree ici et la geometrie vient du
referentiel (`referentiel-comptages-routiers`, 3 739 lignes) une seule fois.
Sans cela le lac ferait des dizaines de Go pour la meme information.

`--all` n'existe pas volontairement : demander quinze annees represente ~2,8 Go de
telechargement. Les annees se prennent une par une, ou par liste.
"""

from __future__ import annotations

import io
import re
import zipfile

# Etend `zipfile` au deflate64. Les archives 2021 et 2023 l'utilisent, les autres
# annees restent en deflate ordinaire : la methode change d'une annee a l'autre
# sans que rien ne le signale, et sans ce greffon Python leve simplement
# "That compression method is not supported".
import zipfile_deflate64  # noqa: F401  (l'import seul applique le correctif)
import polars as pl

from ..lake import write_partition
from .base import Written, client, download, ods_export

name = "paris_road_counts"

BASE = "https://opendata.paris.fr"
LIVE_DATASET = "comptages-routiers-permanents"
HISTO_DATASET = "comptages-routiers-permanents-historique"
REF_DATASET = "referentiel-comptages-routiers"

_ATTACHMENTS = f"{BASE}/api/explore/v2.1/catalog/datasets/{HISTO_DATASET}/attachments"
_YEAR_ZIP = re.compile(r"opendata_txt_(?P<year>\d{4})\.zip$", re.I)

#: La geometrie du troncon, repetee a l'identique sur chaque ligne horaire.
#: C'est de loin la plus grosse colonne ; elle vit dans le referentiel.
_DROP = ("dessin",)


def discover() -> dict[str, str]:
    """annee -> url du ZIP, depuis les pieces jointes du jeu historique."""
    with client() as cli:
        resp = cli.get(_ATTACHMENTS)
        resp.raise_for_status()
        out: dict[str, str] = {}
        for a in resp.json().get("attachments", []):
            m = a.get("metas", {})
            match = _YEAR_ZIP.search(m.get("title") or "")
            if match and m.get("url"):
                out[match.group("year")] = m["url"]
        return dict(sorted(out.items()))


def _parse_member(raw: bytes) -> pl.DataFrame:
    df = pl.read_csv(
        io.BytesIO(raw),
        separator=";",
        infer_schema_length=0,
        truncate_ragged_lines=True,
        quote_char='"',
    )
    df = df.drop([c for c in _DROP if c in df.columns])
    return df.with_columns(
        pl.col("t_1h").str.strip_chars().str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
        pl.col("q").cast(pl.Float64, strict=False),
        pl.col("k").cast(pl.Float64, strict=False),
        pl.col("iu_ac").cast(pl.Int64, strict=False),
    )


def fetch_reference() -> Written:
    """Le referentiel geographique des capteurs -- la geometrie, une seule fois."""
    with client() as cli:
        payload = ods_export(REF_DATASET, base=BASE, cli=cli)
    df = pl.read_csv(
        io.BytesIO(payload), separator=";", infer_schema_length=0, truncate_ragged_lines=True
    )
    path = write_partition(df, "paris_road_sensors", "current")
    return Written("paris_road_sensors", "current", df.height, path, ["geometrie des troncons"])


def fetch_year(year: str, force: bool = False) -> Written:
    available = discover()
    if year not in available:
        raise SystemExit(f"{year} indisponible. Annees publiees : {', '.join(available)}")

    with client() as cli:
        got = download(
            available[year],
            source=name,
            filename=f"opendata_txt_{year}.zip",
            force=force,
            note=f"comptages routiers {year}",
            cli=cli,
        )

    frames: list[pl.DataFrame] = []
    notes: list[str] = []
    if got.from_cache:
        notes.append("octets relus depuis l'archive locale")

    with zipfile.ZipFile(io.BytesIO(got.data)) as z:
        members = [i for i in z.infolist() if not i.is_dir() and i.filename.lower().endswith(".txt")]
        for info in members:
            frames.append(_parse_member(z.read(info)))
    notes.append(f"{len(members)} fichiers hebdomadaires")

    df = pl.concat(frames, how="diagonal_relaxed")
    bad = int(df["t_1h"].is_null().sum())
    if bad > df.height * 0.01:
        raise ValueError(f"{year}: {bad:,}/{df.height:,} horodatages non parses")
    if bad:
        notes.append(f"{bad} horodatages non parses")
    notes.append("colonne `dessin` retiree ; geometrie dans paris_road_sensors")

    path = write_partition(df, name, year)
    return Written(name, year, df.height, path, notes)


def fetch_live(month: str, force: bool = False) -> Written:
    """Une tranche mensuelle du flux courant, avant qu'elle ne sorte de la fenetre.

    `month` au format YYYY-MM. Le decoupage mensuel evite a la fois le plafond de
    10 000 offsets et un export unique de plusieurs Go.
    """
    y, m = month.split("-")
    nxt = f"{int(y) + 1}-01" if m == "12" else f"{y}-{int(m) + 1:02d}"
    where = f"t_1h >= date'{month}-01' AND t_1h < date'{nxt}-01'"

    with client() as cli:
        resp = cli.get(
            f"{BASE}/api/explore/v2.1/catalog/datasets/{LIVE_DATASET}/exports/csv",
            params={"where": where},
        )
        resp.raise_for_status()
        payload = resp.content

    df = pl.read_csv(
        io.BytesIO(payload), separator=";", infer_schema_length=0, truncate_ragged_lines=True
    )
    if df.is_empty():
        return Written(f"{name}_live", month, 0, None, ["aucune ligne -- hors fenetre glissante ?"])

    df = df.drop([c for c in _DROP if c in df.columns]).with_columns(
        pl.col("t_1h").str.to_datetime(strict=False),
        pl.col("q").cast(pl.Float64, strict=False),
        pl.col("k").cast(pl.Float64, strict=False),
    )
    path = write_partition(df, f"{name}_live", month)
    return Written(f"{name}_live", month, df.height, path, [])


def fetch(
    year: str | None = None,
    years: list[str] | None = None,
    month: str | None = None,
    reference: bool = False,
    force: bool = False,
) -> list[Written]:
    if reference:
        return [fetch_reference()]
    if month:
        return [fetch_live(month, force)]
    targets = years or ([year] if year else [])
    if not targets:
        raise SystemExit(
            "precisez --year YYYY (archive 2010-2024), --month YYYY-MM (flux courant) "
            "ou --reference (geometrie des capteurs).\n"
            "Pas de --all : quinze annees representent ~2,8 Go."
        )
    return [fetch_year(y, force) for y in targets]
