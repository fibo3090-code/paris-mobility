"""One module per collector. Each honours the invariants in `parismob.lake`."""

from __future__ import annotations

from types import SimpleNamespace

from . import (
    baac,
    calendar_fr,
    idfm_referentiel,
    idfm_rt_perimeter,
    idfm_stations,
    idfm_surface,
    idfm_validations,
    insee_od,
    meteo,
    ods_bulk,
    paris_events,
    paris_road_counts,
    prim_realtime,
    velib,
)


def _ods(key: str):
    """Expose une entree de `ods_bulk.SPECS` comme un collecteur ordinaire."""
    return SimpleNamespace(
        name=key,
        fetch=lambda force=False, _k=key: ods_bulk.fetch_spec(_k, force=force),
    )


#: CLI name -> module. The short names are what `parismob fetch <name>` accepts.
REGISTRY = {
    # Reseau ferre
    "validations": idfm_validations,
    "surface": idfm_surface,
    # Referentiels
    "stations": idfm_stations,
    "referentiel": idfm_referentiel,
    "rt-perimeter": idfm_rt_perimeter,
    # Contexte
    "calendar": calendar_fr,
    "events": paris_events,
    "meteo": meteo,
    # Offre, qualite, comptages
    "offre": _ods("offre"),
    "qualite": _ods("qualite"),
    "multimodal": _ods("multimodal"),
    "velo-counts": _ods("velo-counts"),
    "chantiers": _ods("chantiers"),
    "transilien": _ods("transilien"),
    "sncf-gares": _ods("sncf-gares"),
    "sncf-ponctualite": _ods("sncf-ponctualite"),
    "air": _ods("air"),
    "accidents": baac,
    "insee-od": insee_od,
    # Route
    "road": paris_road_counts,
    # Temps reel
    "velib": velib,
    "prim": prim_realtime,
}

#: Sources perissables : ce qui n'est archive nulle part et disparait si on ne le
#: collecte pas. Velib' et le flux routier courant ne demandent aucune cle.
PERISHABLE = ("velib", "prim", "events")

__all__ = [
    "REGISTRY",
    "PERISHABLE",
    "calendar_fr",
    "idfm_referentiel",
    "idfm_rt_perimeter",
    "idfm_stations",
    "idfm_surface",
    "idfm_validations",
    "ods_bulk",
    "paris_events",
    "paris_road_counts",
    "prim_realtime",
    "velib",
]
