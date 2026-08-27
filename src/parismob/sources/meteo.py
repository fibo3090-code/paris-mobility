"""Meteo-France quotidien -- le determinant de mobilite qui manquait.

Pluie et temperature deplacent des gens tous les jours, la ou un match de football
en deplace beaucoup un soir tous les deux mois. Pour le velo l'effet est de premier
ordre ; pour les validations il est plus discret mais permanent, et sans lui un
modele attribue a l'evenement ce qui revient a l'averse.

Donnees climatologiques de base, quotidiennes, par departement, Licence Ouverte
v2.0. Un fichier par departement et par periode :

    Q_{dep}_previous-1950-2024_RR-T-Vent.csv.gz
    Q_{dep}_latest-2025-2026_RR-T-Vent.csv.gz

Les huit departements franciliens sont collectes (75, 77, 78, 91, 92, 93, 94, 95).
Plusieurs postes par departement : les stations ne sont pas interchangeables, donc
rien n'est moyenne ici. L'agregation est laissee a la requete, qui seule sait si
elle veut le poste le plus proche d'un lieu ou une moyenne departementale.

Colonnes retenues et pieges
---------------------------
* `AAAAMMJJ` est un **entier**, pas une date : `19500101`. Casté explicitement.
* `RR` precipitations en mm, `TN`/`TX`/`TM` temperatures min/max/moyenne en degres,
  `FFM` vent moyen. Decimales au point, pas a la virgule -- contrairement aux
  profils de validation IDFM.
* Chaque variable a son drapeau qualite `Q*`. Ils sont conserves : une temperature
  douteuse et une temperature absente ne sont pas la meme chose, et ecarter les
  drapeaux ici obligerait a re-telecharger pour en juger plus tard.
* Les premieres decennies sont tres lacunaires -- en 1950 le poste des Innocents
  ne remonte que la pluie. Normal, et sans consequence : la periode utile du
  projet commence en 2015.
"""

from __future__ import annotations

import gzip
import io

import polars as pl

from ..lake import write_partition
from .base import Written, client, download

name = "meteo"

DATASET_API = "https://www.data.gouv.fr/api/1/datasets/?q=donnees+climatologiques+de+base+quotidiennes&page_size=1"

#: Departements franciliens.
DEPARTEMENTS = ("75", "77", "78", "91", "92", "93", "94", "95")

#: Le jeu `RR-T-Vent` porte pluie, temperature et vent : les trois variables qui
#: pesent sur la mobilite. `autres-parametres` ajoute humidite, pression et
#: ensoleillement, non collectes ici faute d'usage identifie.
_PERIODS = ("previous-1950-2024", "latest-2025-2026")

_BASE_URL = (
    "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/synchro_ftp/BASE/QUOT/"
    "Q_{dep}_{period}_RR-T-Vent.csv.gz"
)

#: Colonnes conservees : identite du poste, date, et les mesures utiles avec leur
#: drapeau qualite. Le fichier en compte une centaine, dont l'essentiel decrit des
#: rafales par tranches horaires sans interet ici.
_KEEP = (
    "NUM_POSTE", "NOM_USUEL", "LAT", "LON", "ALTI", "AAAAMMJJ",
    "RR", "QRR", "TN", "QTN", "TX", "QTX", "TM", "QTM",
    "TAMPLI", "DG", "FFM", "QFFM",
)


def _parse(payload: bytes, dep: str) -> pl.DataFrame:
    text = gzip.decompress(payload)
    df = pl.read_csv(
        io.BytesIO(text),
        separator=";",
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )
    df = df.select([c for c in _KEEP if c in df.columns])

    numeric = [c for c in ("RR", "TN", "TX", "TM", "TAMPLI", "DG", "FFM", "LAT", "LON", "ALTI") if c in df.columns]
    return df.with_columns(
        # AAAAMMJJ arrive en entier ; le passer par une chaine evite d'inventer une
        # arithmetique de dates sur un nombre.
        pl.col("AAAAMMJJ").cast(pl.Utf8).str.to_date("%Y%m%d", strict=False).alias("date"),
        *[pl.col(c).cast(pl.Float64, strict=False) for c in numeric],
        pl.lit(dep).alias("departement"),
    ).drop("AAAAMMJJ")


def fetch(force: bool = False, departements: tuple[str, ...] = DEPARTEMENTS) -> list[Written]:
    written: list[Written] = []
    with client() as cli:
        for dep in departements:
            frames: list[pl.DataFrame] = []
            notes: list[str] = []
            for period in _PERIODS:
                url = _BASE_URL.format(dep=dep, period=period)
                try:
                    got = download(
                        url,
                        source=name,
                        filename=f"Q_{dep}_{period}_RR-T-Vent.csv.gz",
                        force=force,
                        note=f"meteo quotidienne {dep} {period}",
                        cli=cli,
                    )
                except Exception as exc:
                    # Une periode absente pour un departement ne doit pas faire
                    # tomber les sept autres.
                    notes.append(f"{period} indisponible ({type(exc).__name__})")
                    continue
                frames.append(_parse(got.data, dep))

            if not frames:
                written.append(Written(name, dep, 0, None, notes or ["aucune donnee"]))
                continue

            df = pl.concat(frames, how="diagonal_relaxed").sort("date")
            bad = int(df["date"].is_null().sum())
            if bad:
                notes.append(f"{bad} dates non parsees")
            notes.append(f"{df['NUM_POSTE'].n_unique()} postes")
            if df.height:
                notes.append(f"{df['date'].min()} -> {df['date'].max()}")

            written.append(
                Written(name, dep, df.height, write_partition(df, name, dep), notes)
            )
    return written
