"""Matrice origine-destination domicile-travail (INSEE) -- les flux, pas les comptages.

Tout le reste du lac mesure des passages a un point : une validation, un capteur,
un totem. Rien ne dit *d'ou a ou* les gens vont. C'est la matrice qui le dit :
pour chaque couple (commune de residence, commune de travail), le nombre d'actifs
occupes de 15 ans et plus qui font ce trajet.

C'est aussi le seul jeu ici directement comparable a ce que produit eqasim. La
population synthetique se valide contre cette matrice ; sans elle on ne peut pas
verifier qu'un scenario reproduit les navettes reelles.

Source : recensement de la population, exploitation complementaire. Le fichier
national fait 53,8 Mo decompresses et couvre toutes les communes ; il est
restreint ici a l'Ile-de-France, ce qui le ramene a quelques centaines de milliers
de lignes.

Pieges
------
* `NBFLUX_C21_ACTOCC15P` est un **effectif pondere**, donc decimal : `52.385`.
  L'arrondir a la collecte perdrait de l'information sur les petits flux, qui sont
  precisement ceux ou la ponderation compte.
* **Paris, Lyon et Marseille sont au niveau arrondissement**, pas commune. Les
  codes parisiens vont de 75101 a 75120 et non `75056`. Un filtre sur les deux
  premiers caracteres les capture correctement, mais toute jointure vers un
  referentiel communal doit en tenir compte.
* Les communes de travail peuvent etre a l'etranger (travailleurs frontaliers) ;
  hors sujet en Ile-de-France mais present dans le fichier national.
"""

from __future__ import annotations

import io
import zipfile

import polars as pl

from ..lake import write_partition
from .base import Written, client, download

name = "insee_od"

#: Millesime -> URL. L'identifiant numerique dans le chemin change a chaque
#: millesime et n'est pas derivable de l'annee. Le separateur avant `csv` change
#: lui aussi -- tiret en 2021, tiret bas en 2022 -- donc les URL sont notees en
#: toutes lettres plutot que construites depuis un modele.
EDITIONS = {
    "2021": "https://www.insee.fr/fr/statistiques/fichier/8201899/base-flux-mobilite-domicile-lieu-travail-2021-csv.zip",
    "2022": "https://www.insee.fr/fr/statistiques/fichier/8582949/base-flux-mobilite-domicile-lieu-travail-2022_csv.zip",
}

#: Prefixes des codes communes franciliens.
IDF_PREFIXES = ("75", "77", "78", "91", "92", "93", "94", "95")


def _flux_column(columns: list[str]) -> str:
    """Le nom de la colonne d'effectif porte le millesime (`NBFLUX_C21_ACTOCC15P`)."""
    for c in columns:
        if c.upper().startswith("NBFLUX"):
            return c
    raise ValueError(f"aucune colonne NBFLUX parmi {columns}")


def fetch(year: str = "2021", both_ends: bool = False, force: bool = False) -> list[Written]:
    """Collecte un millesime, restreint a l'Ile-de-France.

    Par defaut on garde les flux dont **au moins une extremite** est francilienne,
    ce qui conserve les navettes entrantes depuis l'Oise ou l'Eure -- reelles et
    loin d'etre negligeables. `both_ends` restreint aux flux internes a la region.
    """
    url = EDITIONS.get(year)
    if url is None:
        raise SystemExit(f"millesime {year} inconnu. Disponibles : {', '.join(EDITIONS)}")

    with client() as cli:
        got = download(
            url,
            source=name,
            filename=f"base-flux-domicile-travail-{year}.zip",
            force=force,
            note=f"matrice OD domicile-travail {year}",
            cli=cli,
        )

    with zipfile.ZipFile(io.BytesIO(got.data)) as z:
        member = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        raw = z.read(member)

    df = pl.read_csv(
        io.BytesIO(raw),
        separator=";",
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )
    flux = _flux_column(df.columns)

    national = df.height
    dep_res = pl.col("CODGEO").str.slice(0, 2)
    dep_trav = pl.col("DCLT").str.slice(0, 2)
    mask = (
        (dep_res.is_in(list(IDF_PREFIXES)) & dep_trav.is_in(list(IDF_PREFIXES)))
        if both_ends
        else (dep_res.is_in(list(IDF_PREFIXES)) | dep_trav.is_in(list(IDF_PREFIXES)))
    )

    df = (
        df.filter(mask)
        .with_columns(
            pl.col(flux).cast(pl.Float64, strict=False).alias("nb_actifs"),
            dep_res.alias("dep_residence"),
            dep_trav.alias("dep_travail"),
            pl.lit(year).alias("millesime"),
        )
        .drop(flux)
    )

    if df.is_empty():
        return [Written(name, year, 0, None, ["aucun flux francilien -- codes commune inattendus"])]

    total = float(df["nb_actifs"].sum())
    internes = df.filter(pl.col("dep_residence") == pl.col("dep_travail")).height
    path = write_partition(df, name, year)
    return [
        Written(
            name,
            year,
            df.height,
            path,
            [
                f"{df.height:,} couples sur {national:,} nationaux",
                f"{total:,.0f} actifs",
                f"{internes:,} flux intra-departementaux",
            ],
        )
    ]
