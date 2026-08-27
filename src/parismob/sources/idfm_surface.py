"""Validations du reseau de surface -- bus et tram, la moitie manquante du reseau.

Le lac ne contenait que le ferre. Le reseau de surface represente un volume
comparable (~2,8 M lignes/an contre ~1,8 M pour le ferre) et couvre la grande
couronne bien mieux : la ou le ferre s'arrete, le bus continue.

Meme producteur, meme structure, meme piege de forme que le ferre : dix ZIP
annuels 2015-2024 exposes comme champs fichier dans les *enregistrements* de
`histo-validations-reseau-surface`, jamais dans `/attachments`. Le parseur du
ferre est reutilise tel quel -- il a ete ecrit pour absorber dix derives de format
et les memes s'appliquent ici.

Difference notable : sur le reseau de surface l'identifiant utile est la **ligne**
(`ID_GROUPOFLINES` ou equivalent) autant que l'arret, parce qu'un bus valide a
bord et non a l'entree d'une station. Les colonnes sont conservees telles quelles
et canonisees comme pour le ferre ; ce qui ne correspond a aucun alias connu
reste sous son nom d'origine plutot que d'etre silencieusement ecarte.
"""

from __future__ import annotations

import polars as pl

from ..lake import write_partition
from .base import Fetched, Written, client, download, ods_records
from .idfm_validations import parse_zip

name = "idfm_surface"

BASE = "https://data.iledefrance-mobilites.fr"
DATASET = "histo-validations-reseau-surface"

#: Le champ fichier du jeu de surface, par analogie avec `reseau_ferre`.
_FILE_FIELDS = ("reseau_de_surface", "reseau_surface", "surface")


def discover() -> dict[str, str]:
    records = ods_records(DATASET, base=BASE)
    out: dict[str, str] = {}
    for r in records:
        year = str(r.get("annee") or "").strip()
        if not year:
            continue
        for field in _FILE_FIELDS:
            f = r.get(field)
            if isinstance(f, dict) and f.get("url"):
                out[year] = f["url"]
                break
    return dict(sorted(out.items()))


def fetch(year: str | None = None, all_years: bool = False, force: bool = False) -> list[Written]:
    available = discover()
    if not available:
        raise SystemExit(
            f"aucun fichier trouve dans {DATASET}. Le nom du champ fichier a peut-etre change ; "
            f"champs testes : {_FILE_FIELDS}"
        )
    if all_years:
        years = list(available)
    elif year:
        if year not in available:
            raise SystemExit(f"{year} indisponible. Publiees : {', '.join(available)}")
        years = [year]
    else:
        raise SystemExit("precisez --year YYYY ou --all")

    written: list[Written] = []
    with client() as cli:
        for y in years:
            got: Fetched = download(
                available[y],
                source=name,
                filename=f"data-rs-{y}.zip",
                force=force,
                note=f"validations surface {y}",
                cli=cli,
            )
            counts, profiles, notes = parse_zip(got.data, y)
            if got.from_cache:
                notes.insert(0, "octets relus depuis l'archive locale")

            if not counts.is_empty():
                path = write_partition(counts, name, y)
                if "nb_vald_masked" in counts.columns:
                    masked = int(counts["nb_vald_masked"].sum())
                    if masked:
                        notes.append(f"{masked:,} comptages masques conserves en null + drapeau")
                written.append(Written(name, y, counts.height, path, notes))
            if not profiles.is_empty():
                p = write_partition(profiles, f"{name}_profil", y)
                written.append(Written(f"{name}_profil", y, profiles.height, p, []))

            if counts.is_empty() and profiles.is_empty():
                written.append(
                    Written(
                        name,
                        y,
                        0,
                        None,
                        notes + ["aucun membre reconnu -- le nommage des fichiers differe du ferre"],
                    )
                )
    return written
