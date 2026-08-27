# paris-mobility

A mobility data lake for Île-de-France, and eventually an agent-based model built on it.

The goal is to answer what-if questions that a purely statistical model cannot:
what would a new metro line carry, what happens to the highways on the first
Saturday of August, how far does a Stade de France event ripple through the network.

## Status

**Early.** Storage layer written; collectors not yet implemented.
See [`docs/PLAN.md`](docs/PLAN.md) for the full design and the reasoning behind it.

## Approach

The hard part — a synthetic population of the region — is already solved by
[eqasim](https://github.com/eqasim-org/eqasim-france), which builds one for
Île-de-France out of public French data and exports it as a
[MATSim](https://matsim.org/) scenario. This project does not reinvent that.

What it adds is the data eqasim does **not** have: exogenous shocks that move
large numbers of people on known dates. Stadium and arena events, trade fairs,
strike days, school holidays. eqasim synthesises a *typical* weekday; the
interesting questions are all about atypical ones.

## Why the collector comes first

Static open data — census, travel surveys, SIRENE, BD TOPO — is available to
everyone and will still be there next year. It confers no advantage.

Real-time feeds evaporate. Nobody systematically archives IDFM's live disruption
and departure data. Start now and in twelve months this repo holds a record of
planned versus actual service, by line, by minute, that cannot be bought or
backfilled. The edge is not rare data; it is perishable data.

## Design invariants

Three rules every collector honours, enforced in `src/parismob/lake.py`:

1. **Archive raw bytes before parsing.** Parsers are wrong eventually. Re-parse
   rather than re-download — and some feeds are only published for a window.
2. **Manifest every fetch** — url, sha256, byte count, row count, timestamp.
   Provenance you can actually show someone.
3. **Idempotent writes.** One Parquet file per logical partition, replaced whole.
   There is no append path, so there is no way to double-count.

Parquet on disk, DuckDB as the query layer. Nothing loads into RAM until a query
needs it, which keeps the lake workable on modest hardware.

## Sources

| Source | What it gives | Licence |
|---|---|---|
| [IDFM validations](https://data.iledefrance-mobilites.fr/explore/dataset/histo-validations-reseau-ferre/) | Daily entries per station, 2015–2024 — the labels | Open |
| [Que Faire à Paris](https://opendata.paris.fr/explore/dataset/que-faire-a-paris-/) | Events and activities with dates and coordinates | ODbL |
| [Jours fériés](https://calendrier.api.gouv.fr/) | French public holidays | Open (Etalab) |
| [Calendrier scolaire](https://data.education.gouv.fr/explore/dataset/fr-en-calendrier-scolaire/) | School holidays — Paris is Zone C | Open |
| `data/reference/venues.csv` | Curated venue registry: capacity, location, serving stations | This repo |

**Not used:** Google Maps routing data. The
[Maps Platform terms](https://cloud.google.com/maps-platform/terms/maps-service-terms)
prohibit caching, storing, or building derivative datasets from it. Beyond the
licence, a routing ETA is a model output rather than an observation, and feeding
congested travel times into a simulation that exists to *produce* congested travel
times assumes the answer. Routing is handled by self-hosted OSRM or Valhalla on
OpenStreetMap instead.

## Layout

```
src/parismob/
  config.py     paths; override the data root with PARISMOB_DATA
  lake.py       raw archive, Parquet lake, manifest
  sources/      one module per collector
data/
  raw/          untouched bytes + manifest.jsonl   (gitignored)
  lake/         parsed Parquet, partitioned        (gitignored)
  reference/    hand-curated CSVs                  (tracked)
```

## Setup

```bash
uv sync
uv run parismob --help
```

Point `PARISMOB_DATA` at a large disk before collecting in earnest.

## Licence

Not yet chosen. Note that eqasim is GPL, which will matter if its code is
vendored rather than merely run.
