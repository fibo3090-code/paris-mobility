# paris-mobility

A mobility data lake for Île-de-France, and eventually an agent-based model built on it.

The goal is to answer what-if questions that a purely statistical model cannot:
what would a new metro line carry, what happens to the highways on the first
Saturday of August, how far does a Stade de France event ripple through the network.

## Status

**Collector layer built and verified against live sources.** Ten years of rail
validations are in the lake — 18.4M rows, 77 MB of Parquet — along with the station
reference, the French public and school holiday calendars, and a snapshot of the
Paris events feed. Event detection works: the 2019 France home qualifiers score at
the 96th–98th percentile of their year, the 2024 Olympic session days at the 100th.

Not yet running: the PRIM real-time archiver, which is written but needs an API key.
Nothing modelling-related has been started.

- [`docs/PLAN.md`](docs/PLAN.md) — the design and the reasoning, with corrections marked inline.
- [`docs/FINDINGS.md`](docs/FINDINGS.md) — what changed once real data arrived. **Read this first;**
  several of the plan's original numbers were wrong.

```bash
uv sync
uv run parismob status
uv run parismob events --venue stade_de_france --years 2019 \
    --dates 2019-03-25 2019-09-10 2019-10-14 2019-11-14
```

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

`parismob fetch <name>` for each. Full inventory and licence analysis in
[`docs/SOURCES.md`](docs/SOURCES.md).

**Demand — who travelled**

| `fetch` | Source | Rows | Licence |
|---|---|---|---|
| `validations` | [Rail validations](https://data.iledefrance-mobilites.fr/explore/dataset/histo-validations-reseau-ferre/), daily entries per station 2015–2024 | 18,381,014 | Open |
| `surface` | [Bus and tram validations](https://data.iledefrance-mobilites.fr/explore/dataset/histo-validations-reseau-surface/) 2015–2024 | 22,744,218 | Open |
| `transilien` | [Boardings counted on board](https://ressources.data.sncf.com/explore/dataset/comptage-voyageurs-trains-transilien/) — independent of validations | 6,537 | ODbL |

**Supply — what was actually offered**

| `fetch` | Source | Rows | Licence |
|---|---|---|---|
| `offre` | Planned trips per stop, line, time band and weekday — split term-time / school-holiday / summer | 3,303,809 | Open |
| `qualite` | [Punctuality and regularity](https://data.iledefrance-mobilites.fr/explore/dataset/indicateurs-qualite-service-sncf-ratp/) by line and quarter, with contractual target | 18,656 | ODbL |

**Road and cycling**

| `fetch` | Source | Rows | Licence |
|---|---|---|---|
| `road` | [Paris road counters](https://opendata.paris.fr/explore/dataset/comptages-routiers-permanents/) — hourly flow and occupancy. Archive 2010–2024 plus a ~14-month rolling window | ~29 M/yr | ODbL |
| `multimodal` | [Multimodal counts](https://opendata.paris.fr/explore/dataset/comptage-multimodal-comptages/) — bike, scooter, motorcycle, car, truck, bus | 12,314,877 | ODbL |
| `velo-counts` | [Bike counters](https://opendata.paris.fr/explore/dataset/comptage-velo-donnees-compteurs/), hourly | 1,053,636 | ODbL |
| `velib` | [Vélib' live occupancy](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/) — **perishable, no key** | 1,519/poll | ODbL |

**Context and shocks**

| `fetch` | Source | Rows | Licence |
|---|---|---|---|
| `calendar` | Public holidays 2015–2027 and school holidays (Paris is Zone C) | 1,906 | Open |
| `events` | [Que Faire à Paris](https://opendata.paris.fr/explore/dataset/que-faire-a-paris-/) — cultural events, perishable | 3,073 | ODbL |
| `chantiers` | [Paris roadworks](https://opendata.paris.fr/explore/dataset/chantiers-a-paris/) with start and end dates — known in advance | 4,937 | ODbL |

**Reference — the join keys**

| `fetch` | Source | Rows | Licence |
|---|---|---|---|
| `stations` | Station coordinates and the `id_ref_zdc` key | 1,240 | Open |
| `referentiel` | Stop points and stop zones: `arrid → zdaid → zdcid` | 55,949 | Open |
| `rt-perimeter` | Which stops PRIM covers, and the exact SIRI `MonitoringRef` syntax | 74,477 | **Manual download** |
| — | `data/reference/venues.csv` + `venue_capacities.csv` | 28 | This repo |

**Real-time, needs a key**

| `fetch` | Source | Licence |
|---|---|---|
| `prim` | [PRIM SIRI](https://prim.iledefrance-mobilites.fr/) — disruptions and planned-vs-actual to the minute | Registration |

The API key gates **exactly three endpoints** — `stop-monitoring`,
`estimated-timetable`, `general-message`. Everything else above is open. See
[`docs/SOURCES.md`](docs/SOURCES.md) for the analysis, including which datasets are
CC BY-NC-ND (**no derivatives** — do not train on them) and why the national DATEX II
traffic feed does not cover Île-de-France.

The venue registry is the part that cannot be downloaded. Serving stations are
resolved **by distance** from the venue coordinate against the station reference,
never by name: the reference calls it `La Plaine Stade de France - Saint-Denis`,
the validations call it `LA PLAINE-STADE DE FRANCE`, and a substring search for
`SAINT-DENIS` also returns `STRASBOURG-SAINT-DENIS` four miles away. Both files do
agree on `72211`. Run `parismob venues --resolve` to recompute and see drift.

Capacities are recorded per configuration rather than collapsed to one number —
Paris La Défense Arena seats 30,681 for rugby and holds 40,000 for a concert — each
with a source URL, a verification date and a confidence level.

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
  venues.py     the venue registry and its audit path
  analysis.py   event detection -- and why the obvious version is wrong
  cli.py        argparse entrypoint
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

Point `PARISMOB_DATA` at a large disk before collecting in earnest. The full
decade of validations is only 77 MB of Parquet, but the raw archive it is parsed
from is ~150 MB and grows with every real-time poll.

## Commands

```bash
parismob fetch <source> [--year YYYY] [--all] [--force] [--dry-run]
parismob status                       # partitions, rows, size per source
parismob sql "SELECT ..."             # DuckDB over the lake; reference CSVs are views too
parismob venues [--resolve] [--capacities]
parismob events --venue <id> [--years ...] [--dates ...]
parismob probe                        # check PRIM contracts once a key exists
```

Sources: `validations`, `stations`, `calendar`, `events`, `prim`.

## Detecting events

Do not test for event days by looking for an obvious spike. Weekday events at the
Stade de France sit under a ~47,000/day commuter baseline and reach only 1.2× the
annual median — less than an ordinary busy Tuesday — even though they are the
biggest days of the year at those stations. Compare each day against **its own
weekday**, exclude July/August/December from the baseline, and judge on percentile:

```bash
parismob events --venue stade_de_france --years 2019 \
    --dates 2019-03-25 2019-09-10 2019-10-14 2019-11-14
# -> median percentile 97.1, 4/4 in the top decile. PASS
```

Validations count entries only, so marginal validations are a *fraction* of
attendance — roughly 15% for weekday football, roughly 90% for Olympic session
days. Treat capture as a function of event type, not a constant.
[`docs/FINDINGS.md`](docs/FINDINGS.md) has the numbers.

## Licence

Code: MIT — see [LICENSE](LICENSE). Data fetched by the collectors keeps the
licence of its source; see [`docs/SOURCES.md`](docs/SOURCES.md). eqasim is GPL,
which will matter if its code is ever vendored rather than merely run.
