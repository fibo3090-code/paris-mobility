"""Collecteurs Opendatasoft en masse -- offre, qualite de service, comptages, chantiers.

Six jeux qui partagent la meme mecanique : un export complet, archive puis ecrit
en une partition. Ils sont regroupes ici plutot qu'eclates en six modules quasi
identiques ; ce qui les distingue tient dans la table `SPECS` ci-dessous.

Pourquoi l'export et non `/records`
-----------------------------------
Opendatasoft refuse tout `offset` au-dela de 10 000. Quatre de ces jeux depassent
le million de lignes, ce qui rend la pagination impossible et non pas seulement
lente. `/exports/csv` ignore ce plafond et tient en une requete.

Les jeux notables
-----------------
**L'offre hebdomadaire moyenne** (3,3 M lignes sur trois jeux) est le complement
manquant des validations : nombre de courses par arret, ligne, tranche horaire et
jour de semaine. Les validations mesurent la demande, ceci mesure l'offre, et leur
rapport donne une charge -- ce qui n'etait pas modelisable jusqu'ici.

Son decoupage en trois versions -- hors vacances, vacances scolaires, vacances
d'ete -- est en soi une validation empirique des variables calendaires : IDFM
planifie deja son offre selon cette typologie. Les trois vont dans la meme table,
distinguees par la colonne `regime`.

**Les indicateurs de qualite de service** donnent ponctualite et regularite par
ligne et par trimestre, avec l'objectif contractuel et la mention de penalite.
C'est le seul historique de service realise accessible sans cle PRIM : granularite
bien plus grossiere, mais il remonte dans le passe la ou PRIM ne peut que partir
de maintenant.

**Le comptage multimodal** (12,3 M lignes) est la seule source de la region qui
separe velos, trottinettes, deux-roues motorises, VL, PL et autobus.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field

import polars as pl

from ..lake import raw_path, record_fetch, sha256_bytes, write_partition
from .base import Written, client, ods_export

name = "ods_bulk"

IDFM = "https://data.iledefrance-mobilites.fr"
PARIS = "https://opendata.paris.fr"
SNCF = "https://ressources.data.sncf.com"


@dataclass(frozen=True)
class Spec:
    key: str
    """Nom court accepte par `parismob fetch <key>`."""
    table: str
    """Table cible dans le lac."""
    base: str
    datasets: dict[str, str] = field(default_factory=dict)
    """partition -> dataset_id. Plusieurs entrees fusionnent dans la meme table."""
    label_column: str | None = None
    """Colonne ajoutee pour distinguer les jeux fusionnes."""
    note: str = ""
    slice_column: str | None = None
    """Colonne de date sur laquelle decouper. Voir `_collect_sliced`."""
    min_year: int = 2018
    """Borne basse du decoupage, pour ignorer les horodatages aberrants."""


SPECS: dict[str, Spec] = {
    "offre": Spec(
        key="offre",
        table="idfm_offre",
        base=IDFM,
        datasets={
            "hors_vacances": "offre_hebdomadaire_moyenne_hors_vacances",
            "vacances_scolaires": "offre_hebdomadaire_moyenne_vacances_scolaires",
            "vacances_ete": "offre_hebdomadaire_moyenne_vacances_ete",
        },
        label_column="regime",
        note="offre planifiee : courses par arret, ligne, tranche horaire, jour",
    ),
    "qualite": Spec(
        key="qualite",
        table="idfm_qualite_service",
        base=IDFM,
        datasets={
            "sncf_ratp": "indicateurs-qualite-service-sncf-ratp",
            "parcours": "indicateurs-qualite-service-parcours-voyageur",
        },
        label_column="source_indicateur",
        note="ponctualite et regularite par ligne et trimestre",
    ),
    "multimodal": Spec(
        key="multimodal",
        table="paris_multimodal",
        base=PARIS,
        datasets={"comptages": "comptage-multimodal-comptages"},
        note="velo, trottinette, 2RM, VL, PL, autobus",
        # 12,3 M lignes. L'export global ne repond pas dans un delai utilisable ;
        # decoupe en mois il rend ~300 k lignes en ~35 s. Voir `_collect_sliced`.
        slice_column="t",
    ),
    "velo-counts": Spec(
        key="velo-counts",
        table="paris_comptage_velo",
        base=PARIS,
        datasets={"compteurs": "comptage-velo-donnees-compteurs"},
        note="comptages velo horaires par totem",
    ),
    "chantiers": Spec(
        key="chantiers",
        table="paris_chantiers",
        base=PARIS,
        datasets={"courant": "chantiers-a-paris"},
        note="travaux avec dates de debut et de fin -- choc connu a l'avance",
    ),
    "transilien": Spec(
        key="transilien",
        table="sncf_montants_transilien",
        base=SNCF,
        datasets={"montants": "comptage-voyageurs-trains-transilien"},
        note="montees comptees a bord -- mesure independante des validations",
    ),
    "sncf-gares": Spec(
        key="sncf-gares",
        table="sncf_gares",
        base=SNCF,
        datasets={
            "frequentation": "frequentation-gares",
            "liste": "liste-des-gares",
        },
        label_column="jeu",
        note="frequentation annuelle et referentiel des gares SNCF",
    ),
    "sncf-ponctualite": Spec(
        key="sncf-ponctualite",
        table="sncf_ponctualite",
        base=SNCF,
        datasets={
            "transilien": "ponctualite-mensuelle-transilien",
            "intercites": "regularite-mensuelle-intercites",
            "tgv": "regularite-mensuelle-tgv-aqst",
        },
        label_column="reseau",
        # Complement mensuel des indicateurs trimestriels IDFM : meme idee, maille
        # plus fine, et une mesure produite par le transporteur plutot que par
        # l'autorite organisatrice.
        note="regularite mensuelle par ligne",
    ),
    "air": Spec(
        key="air",
        table="idfm_qualite_air",
        base=IDFM,
        datasets={"reseau": "qualite-de-lair-dans-le-reseau-de-transport-francilien"},
        note="qualite de l'air mesuree en station",
    ),
}


def _collect(spec: Spec, dataset: str, partition: str, cli) -> Written:
    payload = ods_export(dataset, base=spec.base, cli=cli)
    dest = raw_path(spec.table, f"{dataset}.csv")
    dest.write_bytes(payload)

    df = pl.read_csv(
        io.BytesIO(payload),
        separator=";",
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )
    record_fetch(
        source=spec.table,
        url=f"{spec.base}/api/explore/v2.1/catalog/datasets/{dataset}/exports/csv",
        path=dest,
        n_bytes=len(payload),
        digest=sha256_bytes(payload),
        rows=df.height,
        note=spec.note,
    )
    if spec.label_column:
        df = df.with_columns(pl.lit(partition).alias(spec.label_column))

    if df.is_empty():
        return Written(spec.table, partition, 0, None, [f"{dataset} : export vide (jeu restreint ?)"])

    path = write_partition(df, spec.table, partition)
    return Written(spec.table, partition, df.height, path, [f"{dataset}"])


def _months(start: str, end: str) -> list[str]:
    """Liste des mois YYYY-MM de `start` a `end` inclus."""
    y, m = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def _get_with_retry(cli, url: str, params: dict, *, label: str, attempts: int = 4):
    """GET avec reessais sur coupure reseau.

    Les exports mensuels durent une trentaine de secondes chacun et le serveur
    ferme parfois la connexion en cours de transfert
    (`peer closed connection without sending complete message body`). Sur une
    collecte de soixante-dix mois, cela finit par arriver ; sans reessai une seule
    coupure abandonne tout le reste.

    Les erreurs HTTP explicites ne sont pas reessayees : un 4xx ne s'ameliore pas
    en insistant, et le masquer donnerait un jeu incomplet passant pour complet.
    """
    import time

    import httpx

    last: Exception | None = None
    for attempt in range(attempts):
        try:
            r = cli.get(url, params=params)
            r.raise_for_status()
            return r
        except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ReadTimeout, httpx.ConnectError) as exc:
            last = exc
            if attempt < attempts - 1:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"{label}: abandon apres {attempts} tentatives ({type(last).__name__}: {last})")


def _collect_sliced(spec: Spec, dataset: str, cli, force: bool = False) -> list[Written]:
    """Collecte mois par mois les jeux trop volumineux pour un export unique.

    L'export global de `comptage-multimodal-comptages` (12,3 M lignes) ne rend pas
    la main dans un delai exploitable, tandis qu'une tranche mensuelle repond en
    une trentaine de secondes. Le decoupage n'est donc pas une optimisation mais
    la seule facon d'obtenir le jeu.

    Chaque mois est sa propre partition, ce qui rend la reprise gratuite : un mois
    deja ecrit est saute, et une interruption ne coute que le mois en cours.

    La borne basse existe parce que la source contient des horodatages aberrants
    -- le minimum publie est l'an **0001**. Les balayer par annee reelle plutot que
    de partir du minimum evite des milliers de requetes vides.
    """
    resp = cli.get(
        f"{spec.base}/api/explore/v2.1/catalog/datasets/{dataset}/records",
        params={"select": f"max({spec.slice_column}) as fin", "limit": 1},
    )
    resp.raise_for_status()
    results = resp.json().get("results") or [{}]
    end_raw = str(results[0].get("fin") or "")[:7]
    if not end_raw:
        raise SystemExit(f"{dataset}: impossible de determiner la borne haute")

    from ..config import LAKE_DIR

    months = _months(f"{spec.min_year}-01", end_raw)
    out: list[Written] = []
    for month in months:
        # Reprise. Un mois passe est fige, donc une partition deja ecrite n'a pas
        # besoin d'etre refaite -- ce qui rend une interruption reseau gratuite.
        # Le dernier mois est toujours refait : il est encore en cours d'ecriture
        # a la source. `force` refait tout.
        existing = LAKE_DIR / spec.table / f"{month}.parquet"
        if existing.exists() and not force and month != months[-1]:
            continue

        y, m = month.split("-")
        nxt = f"{int(y) + 1}-01" if m == "12" else f"{y}-{int(m) + 1:02d}"
        where = (
            f"{spec.slice_column} >= date'{month}-01' AND "
            f"{spec.slice_column} < date'{nxt}-01'"
        )
        r = _get_with_retry(
            cli,
            f"{spec.base}/api/explore/v2.1/catalog/datasets/{dataset}/exports/csv",
            {"where": where},
            label=f"{dataset} {month}",
        )
        df = pl.read_csv(
            io.BytesIO(r.content),
            separator=";",
            infer_schema_length=0,
            truncate_ragged_lines=True,
        )
        if df.is_empty():
            continue
        dest = raw_path(spec.table, f"{dataset}-{month}.csv")
        dest.write_bytes(r.content)
        record_fetch(
            source=spec.table,
            url=f"{spec.base}/api/explore/v2.1/catalog/datasets/{dataset}/exports/csv?{where}",
            path=dest,
            n_bytes=len(r.content),
            digest=sha256_bytes(r.content),
            rows=df.height,
            note=f"{spec.note} ({month})",
        )
        out.append(
            Written(spec.table, month, df.height, write_partition(df, spec.table, month), [])
        )
    return out


def fetch_spec(key: str, force: bool = False) -> list[Written]:
    spec = SPECS.get(key)
    if spec is None:
        raise SystemExit(f"jeu inconnu {key!r}. Connus : {', '.join(SPECS)}")
    with client() as cli:
        if spec.slice_column:
            out: list[Written] = []
            for ds in spec.datasets.values():
                out.extend(_collect_sliced(spec, ds, cli, force=force))
            return out
        return [_collect(spec, ds, part, cli) for part, ds in spec.datasets.items()]
