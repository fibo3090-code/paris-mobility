"""The venue registry -- the one asset here that cannot be downloaded.

Everything else in the lake is somebody else's open data. This table is the
project's own contribution: which major venues exist, where they are, how many
people they hold, and -- the part that does the work -- which stations serve them,
expressed as numeric IDFM stop identifiers rather than names.

Why identifiers and not names
-----------------------------
The station reference calls it `La Plaine Stade de France - Saint-Denis`. The
validations files call it `LA PLAINE-STADE DE FRANCE`. Neither string matches the
other, and a substring search for `SAINT-DENIS` additionally returns
`STRASBOURG-SAINT-DENIS`, a metro stop in central Paris four miles away with no
connection to the stadium. Both files do agree on `72211`.

Serving stations were resolved by distance from the venue coordinate against
`emplacement-des-gares-idf`, not by hand and not by name, so the derivation is
reproducible: `parismob venues --resolve` recomputes it and reports drift.

Capacities live in a separate table because several venues have several. Paris La
Defense Arena seats 30,681 for rugby and holds 40,000 for a concert; Roland-Garros
spreads its attendance across courts and across a fortnight. Collapsing those to
one number per venue would be inventing data, so each row states the configuration
it refers to, its unit, its source, and how far the source should be trusted.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from .lake import read_reference

VENUES_CSV = "venues.csv"
CAPACITIES_CSV = "venue_capacities.csv"


@dataclass(frozen=True)
class Venue:
    venue_id: str
    name: str
    commune: str
    lat: float
    lon: float
    serving_zdc: tuple[int, ...]
    radius_m: int
    notes: str


def _split_zdc(raw: str | None) -> tuple[int, ...]:
    if not raw:
        return ()
    return tuple(int(p) for p in str(raw).split("|") if p.strip().isdigit())


def load() -> list[Venue]:
    df = read_reference(VENUES_CSV)
    return [
        Venue(
            venue_id=r["venue_id"],
            name=r["name"],
            commune=r["commune"],
            lat=float(r["lat"]),
            lon=float(r["lon"]),
            serving_zdc=_split_zdc(r["serving_zdc"]),
            radius_m=int(r["radius_m"]),
            notes=r["notes"] or "",
        )
        for r in df.iter_rows(named=True)
    ]


def capacities() -> pl.DataFrame:
    return read_reference(CAPACITIES_CSV)


def venue(venue_id: str) -> Venue:
    for v in load():
        if v.venue_id == venue_id:
            return v
    raise KeyError(f"unknown venue {venue_id!r}. Known: {', '.join(v.venue_id for v in load())}")


def serving_zdc(venue_id: str) -> tuple[int, ...]:
    return venue(venue_id).serving_zdc


def all_serving_zdc() -> list[int]:
    """Every station that serves any venue, deduplicated."""
    seen: dict[int, None] = {}
    for v in load():
        for z in v.serving_zdc:
            seen[z] = None
    return list(seen)


def monitoring_refs() -> list[str]:
    """SIRI MonitoringRef values for the venue-serving stations, for PRIM.

    Resolved against the real-time perimeter rather than constructed. An earlier
    version of this function built `STIF:StopArea:SP:<zda>:` strings from the
    station reference; the perimeter file shows the actual syntax is
    `STIF:StopPoint:Q:<arrid>:`, addressing individual quays, so every one of
    those would have failed. Constructing identifiers for someone else's API is
    guesswork -- this reads the list they publish.

    The chain is venue ZDC -> ZDA -> StopPoint -> the subset PRIM actually covers.
    Filtering on that last step matters: polling a stop outside the perimeter
    returns an empty response rather than an error, which is indistinguishable
    from a stop where nothing is running.

    Returns an empty list rather than raising when the prerequisites are missing,
    so a PRIM poll degrades to disruptions-only instead of dying.
    """
    from .config import LAKE_DIR

    perimeter = LAKE_DIR / name_of_perimeter_partition()
    arrets = LAKE_DIR / "idfm_arrets" / "current.parquet"
    zones = LAKE_DIR / "idfm_zones_arrets" / "current.parquet"
    if not (perimeter.exists() and arrets.exists() and zones.exists()):
        return []

    wanted = all_serving_zdc()
    zda = (
        pl.read_parquet(zones)
        .filter(pl.col("zdcid").is_in(wanted))
        .select("zdaid")
        .unique()
    )
    stoppoints = (
        pl.read_parquet(arrets).join(zda, on="zdaid", how="inner").select("arrid").unique()
    )
    covered = (
        pl.read_parquet(perimeter)
        .join(stoppoints, on="arrid", how="inner")
        .select("monitoring_ref")
        .unique()
        .sort("monitoring_ref")
    )
    return covered["monitoring_ref"].to_list()


def name_of_perimeter_partition() -> str:
    return "idfm_rt_perimeter/current.parquet"


def resolve(radius_override: float | None = None) -> pl.DataFrame:
    """Recompute serving stations from coordinates and diff against the registry.

    This is the audit path. If IDFM adds a station -- as it did at Saint-Denis
    Pleyel in 2024 -- the registry is stale until someone notices, and noticing is
    what this is for.
    """
    from .sources.idfm_stations import stations_near

    rows = []
    for v in load():
        near = stations_near(v.lat, v.lon, radius_override or v.radius_m)
        found = set(int(z) for z in near["id_ref_zdc"])
        recorded = set(v.serving_zdc)
        rows.append(
            {
                "venue_id": v.venue_id,
                "recorded": len(recorded),
                "within_radius": len(found),
                "missing_from_registry": ", ".join(
                    str(z) for z in sorted(found - recorded)
                ) or "-",
                "recorded_but_outside_radius": ", ".join(
                    str(z) for z in sorted(recorded - found)
                ) or "-",
            }
        )
    return pl.DataFrame(rows)
