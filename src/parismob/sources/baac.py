"""Accidents corporels de la circulation (BAAC) -- Ministere de l'interieur.

Tout accident corporel enregistre par les forces de l'ordre, geolocalise, depuis
2005. Quatre tables par annee, liees par `Num_Acc` :

    caracteristiques  -- date, heure, lumiere, meteo, type de collision, position
    lieux             -- categorie de route, nombre de voies, profil, surface
    usagers           -- une ligne par personne : gravite, categorie, trajet
    vehicules         -- une ligne par vehicule : categorie, manoeuvre, obstacle

Pour ce projet l'interet est double. Les accidents sont un **choc exogene local**
qui perturbe la circulation a un endroit et une heure connus, ce qui en fait un
regresseur pour les comptages routiers. Et le champ `trajet` des usagers
distingue domicile-travail, domicile-ecole, courses et loisirs -- une des rares
sources publiques qui qualifie le *motif* d'un deplacement observe.

Le nommage des fichiers est incoherent d'une annee a l'autre
------------------------------------------------------------
Quatre conventions coexistent, plus une faute de frappe conservee a la source :

    caracteristiques-2018.csv     mot complet, minuscules, tiret
    carcteristiques-2021.csv      <- 'a' manquant, tel quel chez le producteur
    caract-2023.csv               abrege
    Caract_2024.csv               capitale et tiret bas

Les fichiers sont donc apparies par motif tolerant plutot que par nom construit.
Un nom fabrique a partir d'un modele echouerait sur trois annees sur sept, et
echouerait en silence -- une annee manquante ressemble a une annee sans accident.
"""

from __future__ import annotations

import io
import re

import polars as pl

from ..lake import write_partition
from .base import Written, client, download

name = "baac"

DATASET_API = "https://www.data.gouv.fr/api/1/datasets/53698f4ca3a729239d2036df/"

#: Les quatre tables. Pour les caracteristiques on se contente du prefixe `car`
#: suivi du millesime : aucune autre table ne commence ainsi, et enumerer les
#: orthographes exactes echouait sur la faute de frappe -- `carcteristiques-2021`
#: n'a pas le second `a`, ce qui avait fait perdre 2021 et 2022 en silence.
_TABLES = {
    "caracteristiques": re.compile(r"^car[^-_]*[-_]?(?P<year>\d{4})", re.I),
    "lieux": re.compile(r"^lieux[-_]?(?P<year>\d{4})", re.I),
    "usagers": re.compile(r"^usagers[-_]?(?P<year>\d{4})", re.I),
    "vehicules": re.compile(r"^vehicules[-_]?(?P<year>\d{4})", re.I),
}

#: Departements franciliens, pour reduire un jeu national a la region.
IDF_DEPS = {"75", "77", "78", "91", "92", "93", "94", "95"}


def discover() -> dict[tuple[str, str], str]:
    """(table, annee) -> url, apparie par motif et non par nom construit."""
    with client() as cli:
        resp = cli.get(DATASET_API)
        resp.raise_for_status()
        out: dict[tuple[str, str], str] = {}
        for r in resp.json().get("resources", []):
            title = (r.get("title") or "").strip()
            if not title.lower().endswith(".csv"):
                continue
            stem = title[:-4]
            for table, pattern in _TABLES.items():
                m = pattern.match(stem)
                if m:
                    out[(table, m.group("year"))] = r.get("url", "")
                    break
        return out


def _sniff_separator(header: str) -> str:
    counts = {sep: header.count(sep) for sep in (";", ",", "\t", "|")}
    return max(counts, key=lambda k: counts[k])


def _parse(payload: bytes) -> pl.DataFrame:
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            text = payload.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = payload.decode("latin-1", errors="replace")

    first = text.split("\n", 1)[0]
    return pl.read_csv(
        io.BytesIO(text.encode("utf-8")),
        separator=_sniff_separator(first),
        infer_schema_length=0,
        truncate_ragged_lines=True,
    ).rename(lambda c: c.strip().lower())


def fetch(
    year: str | None = None,
    all_years: bool = False,
    idf_only: bool = True,
    force: bool = False,
) -> list[Written]:
    """Collecte une annee ou toutes. Filtre l'Ile-de-France par defaut.

    Le jeu est national ; `idf_only` restreint les caracteristiques aux huit
    departements franciliens. Les trois autres tables n'ont pas de departement et
    sont conservees entieres -- elles se filtrent par jointure sur `num_acc`.
    """
    available = discover()
    years = sorted({y for _, y in available})
    if all_years:
        targets = years
    elif year:
        if year not in years:
            raise SystemExit(f"{year} indisponible. Annees : {', '.join(years)}")
        targets = [year]
    else:
        raise SystemExit(f"precisez --year YYYY ou --all. Annees : {', '.join(years)}")

    written: list[Written] = []
    with client() as cli:
        for y in targets:
            for table in _TABLES:
                url = available.get((table, y))
                if not url:
                    written.append(
                        Written(f"{name}_{table}", y, 0, None, ["absent du catalogue"])
                    )
                    continue
                got = download(
                    url,
                    source=name,
                    filename=f"{table}-{y}.csv",
                    force=force,
                    note=f"BAAC {table} {y}",
                    cli=cli,
                )
                df = _parse(got.data)
                notes = []

                if table == "caracteristiques" and idf_only and "dep" in df.columns:
                    before = df.height
                    # Le departement est note '75' recemment, '750' avant 2019 --
                    # le code INSEE etait suivi d'un zero. Les deux formes sont
                    # ramenees a deux caracteres avant comparaison.
                    df = df.with_columns(
                        pl.col("dep").str.strip_chars().str.replace(r"^(\d{2})0$", "${1}").alias("dep")
                    ).filter(pl.col("dep").is_in(list(IDF_DEPS)))
                    notes.append(f"Ile-de-France : {df.height:,} sur {before:,} lignes")

                if df.is_empty():
                    written.append(Written(f"{name}_{table}", y, 0, None, notes + ["vide"]))
                    continue
                path = write_partition(df, f"{name}_{table}", y)
                written.append(Written(f"{name}_{table}", y, df.height, path, notes))
    return written
