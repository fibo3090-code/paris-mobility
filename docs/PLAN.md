# Paris mobility data lake — collector foundation

## Context

The goal is an agent-based mobility model of Île-de-France that can answer
what-if questions ("what would this metro line carry", "what happens to the A6 on
the first Saturday of August"). The `eqasim` pipeline already builds the hard part
— a synthetic population of the region — so the project's own contribution has to
be the data eqasim does *not* have.

This plan covers the foundation only: a collector and storage layer that starts
accumulating that data now. It is deliberately unambitious in modelling terms and
deliberately careful in engineering terms, because the value compounds with time
and a collector that silently corrupts itself for six months is worse than none.

Two data ideas were raised. One is better than it looks; the other is blocked.
Both are resolved below, because they change what gets built.

> **Status: built and verified.** The collector layer described here is implemented
> and has been run end to end against live sources. Ten years of validations
> (18.4M rows, 77 MB of Parquet) are in the lake and the acceptance test passes.
>
> **This document was written before any data was downloaded, and several of its
> factual claims turned out to be wrong** — including the download path, the schema,
> and the size of the effect it tells you to look for. Corrections are inline below,
> marked **[corrected]**, and the full account is in
> [`FINDINGS.md`](FINDINGS.md). Read that before trusting any specific
> number in here.

---

## Decision 1 — Event calendars: yes, and they're the centrepiece

Better than it appears, for three reasons:

- **Known in advance.** Most predictive features are lagged — you learn them after
  the fact. Fixture lists and concert calendars publish months ahead, so they work
  for genuine forecasting, not just post-hoc explanation.
- **Enormous and sharp.** A match at the Parc des Princes puts ~48k people onto M9/M10
  inside forty minutes. Stade de France puts ~80k onto RER B and D. That dwarfs the
  weather effects most people model first.

  **[corrected]** Attendance is not what the data sees. Validations count *entries
  only* — attendees tap in at their origin on the way there, and only pass a stadium
  gate going home. A weekday international at the Stade de France yields **+10,000
  to +13,000** marginal validations across four stations, not 80,000. The capture
  ratio ranges from ~15% (weekday football) to ~90% (Olympic session days) and must
  be modelled as a function of event and day type, not a constant.
- **Absent from eqasim**, which synthesises a *typical* weekday. Events are precisely
  the extension that makes the model original rather than a re-run.

Same category, systematically ignored, worth collecting alongside:

| Signal | Why it matters |
|---|---|
| Trade fairs (Porte de Versailles, Villepinte) | 100k+ visitors over multi-day runs; almost nobody models them |
| Strike days (RATP/SNCF) | Among the largest single-day effects in the data |
| University term and exam dates | Moves a large, spatially concentrated population |

**Immediate validation, week one:** the validations dataset already covers 2015–2024.
Join Stade de France station counts against known match dates — the spike should be
visible by eye. If it isn't, the pipeline is broken, and you'll know early.

**[corrected] — this test as written fails on a working pipeline.** Weekday events
are buried under a ~47,000/day commuter baseline at La Plaine. The four France home
qualifiers of 2019 were the four biggest days of the year at those stations, and
still only reached 1.20–1.27× the annual median, while an ordinary Tuesday reached
1.31×. Normalise by day-of-week, exclude Jul/Aug/Dec from the baseline, and judge on
percentile rather than ratio; then the same four dates land at the 96th–98th
percentile and pass unambiguously. Implemented in `analysis.py`; run it with
`parismob events --venue stade_de_france --years 2019 --dates ...`.

---

## Decision 2 — Google Maps routes: blocked, and the wrong input anyway

**Legal.** The [Maps Platform terms](https://cloud.google.com/maps-platform/terms/maps-service-terms)
prohibit pre-fetching, caching, indexing or storing content, and prohibit building
derivative datasets. The Routes API permits caching lat/lon for at most 30 days. A
historical route archive is exactly the prohibited case. Consequence is key revocation
and results you can't publish — not a technicality.

**Three reasons to avoid it even if it were permitted:**

1. Google's ETA is itself a model output. You would learn to imitate Google's
   predictions rather than observe reality.
2. Cost. A meaningful OD matrix polled every 15 minutes is billed per element and
   becomes astronomically expensive.
3. Double-counting. MATSim *produces* congested travel times. Feeding congested times
   in as input assumes the answer you're trying to compute.

**Use instead:**

- **Routing** — self-hosted [OSRM](https://project-osrm.org/) or
  [Valhalla](https://valhalla.github.io/valhalla/) on OSM. Free, unlimited, deterministic,
  Docker, fine on 32 GB. Gives free-flow times and capacities, which is what the
  simulation actually wants.
- **Observed congestion** — [TomTom Traffic Stats](https://developer.tomtom.com/traffic-stats/documentation/product-information/introduction)
  is explicitly licensed for historical analysis. Free alternatives: the
  [Paris permanent counters](https://opendata.paris.fr/explore/dataset/comptages-routiers-permanents/)
  (3,000+ segments, hourly, back to 2010) and the
  [national road network feed](https://transport.data.gouv.fr/datasets/etat-de-circulation-en-temps-reel-sur-le-reseau-national-routier-non-concede).
- **Sanity check** — if a Google comparison is wanted, sample a handful of OD pairs
  live through the official API and don't archive the responses.

---

## Decision 3 — The actual edge is ephemerality, not exoticism

Static open data (census, surveys, SIRENE, BD TOPO) is available to everyone and will
still be there next year. It confers no advantage.

Real-time feeds evaporate. IDFM's PRIM platform publishes live disruption messages and
next-departure data (Navitia-backed, 20k requests/day). Nobody archives it systematically.
Start now and in twelve months the project owns a record of *planned versus actual service,
by line, by minute* that cannot be bought or backfilled. Same logic for Vélib' occupancy,
live road counters, and event calendars — which are deleted once the event passes.

That is the answer to "what unexpected data": not rare sources, but perishable ones.
This is why the collector is stop zero and not an afterthought.

---

## Decision 4 — Hardware

Target box: 32 GB RAM, RTX 4070 (12 GB VRAM).

- **MATSim is comfortable** at a 5–10% regional sample. It is JVM, CPU-bound and
  multithreaded.
- **The 4070 does nothing for MATSim.** There is no GPU path. Budget CPU cores and RAM,
  not VRAM.
- The GPU earns its place later, in spatio-temporal graph neural networks for
  network-wide forecasting (the STGCN / Graph WaveNet / DCRNN lineage) — a legitimate
  stage 2.5.
- For tabular calendar-and-event features, gradient boosting on CPU will most likely
  beat a transformer. Don't let the GPU pick the architecture.
- Practical: develop on the 4070 box, keep the repo in git, point `PARISMOB_DATA` at a
  large disk. `config.py` already reads that variable.

---

## Build

### Architecture

```
src/parismob/
  config.py            # paths; PARISMOB_DATA override            [written]
  lake.py              # raw archive + Parquet + manifest         [written]
  cli.py               # argparse entrypoint
  venues.py            # venue registry loader
  sources/
    base.py            # Source protocol: name, fetch(), parse()
    idfm_validations.py
    paris_events.py
    calendar_fr.py
data/
  raw/                 # untouched bytes + manifest.jsonl (gitignored)
  lake/                # Parquet, partitioned by source (gitignored)
  reference/venues.csv # hand-curated, committed to git
```

Three invariants, already implemented in `lake.py` and to be honoured by every collector:

1. **Archive raw bytes before parsing.** Parsers are wrong eventually; re-parse rather
   than re-download.
2. **Manifest every fetch** — url, sha256, bytes, rows, timestamp. Provenance you can show.
3. **Idempotent writes.** One Parquet file per logical partition, replaced whole. No
   append path means no double-counting.

DuckDB reads the Parquet directly off disk, so the lake stays queryable on either machine.

### Collectors

All four endpoints verified live during planning.

**`idfm_validations`** — the labels.
[Attachment index](https://data.iledefrance-mobilites.fr/explore/dataset/histo-validations-reseau-ferre/)
lists one ZIP per year, 2015–2024. Fetch index → download ZIPs → extract → parse.

**[corrected] The `/attachments` endpoint holds one PDF and no data.** The ZIPs are
file-typed fields inside the dataset's ten *records*; fetch them from
`/records`, reading `reseau_ferre.url`. A collector pointed at `/attachments`
finds nothing and reports success.

- Tab-separated `.txt`, not CSV. Two file kinds per period: `*_NB_FER` (daily counts),
  `*_PROFIL_FER` (hourly shape).
- `NB_FER` columns: `JOUR, CODE_STIF_TRNS, CODE_STIF_RES, CODE_STIF_ARRET, LIBELLE_ARRET,
  ID_ZDC, CATEGORIE_TITRE, NB_VALD`
- `PROFIL_FER` columns: `..., CAT_JOUR, TRNC_HORR_60, Pourcentage_validations`
- **Known traps:** period naming drifts between semesters (`S1`) and quarters (`T3`, `T4`)
  within the same year; `NB_VALD` contains masked strings like `"Moins de 5"` for small
  counts, so the column is not natively numeric.
- Scale: ~2M rows/year, ~20M total, 15 MB zipped → ~123 MB text per year.

**[corrected] Ten format drifts, not two.** The stop id column is `ID_REFA_LDA`,
`lda` or `ID_ZDC` depending on the year — never only `ID_ZDC`. Separators, file
extensions, encodings (including a UTF-16 member and two BOM variants), a stray
space in a 2023 filename, nested 2015–2019 archives inside the 2020 ZIP, and
**three different date formats inside 2024 alone** all have to be handled. Masked
`NB_VALD` values occur in 2015–2017 only. Actual scale is 18.4M rows, 77 MB of
Parquet for the decade. Full list and the two silent-corruption cases:
[`FINDINGS.md`](FINDINGS.md).

The stop id joins to `id_ref_zdc` in the
[stations reference](https://data.iledefrance-mobilites.fr/explore/dataset/emplacement-des-gares-idf/),
which is what makes the venue registry work.
- `CAT_JOUR` encodes IDFM's own day typology (working day / school-holiday working day /
  Saturday / Sunday-and-holiday) — direct evidence that the calendar features matter.

**`paris_events`** — [Que Faire à Paris](https://opendata.paris.fr/explore/dataset/que-faire-a-paris-/api/),
ODbL, ~3,054 records. Opendatasoft Explore v2.1 API. Useful fields: `title`, `date_start`,
`date_end`, `occurrences`, `lat_lon`, `address_*`, `audience`.
**Traps:** gzip must be handled (`--compressed` equivalent); some rows have null dates and
`lat_lon` of `{0.0, 0.0}`. Culture-focused — it will *not* reliably carry PSG fixtures or
Stade de France concerts, which is why the venue registry exists separately.

**`calendar_fr`** — the strongest cheap features.
- Jours fériés: `https://calendrier.api.gouv.fr/jours-feries/metropole/{year}.json` (Etalab)
- School holidays: `fr-en-calendrier-scolaire` on data.education.gouv.fr. **Paris is Zone C**;
  filter on the `zones` field. Fields: `description, start_date, end_date, location, zones,
  annee_scolaire`.

**`venues.csv`** — hand-curated reference, the differentiating asset. One row per major venue:
name, lat/lon, capacity, serving stations (as IDFM stop identifiers so it joins to validations),
plus `capacity_source` and `verified_on` columns.

Seed set: Stade de France, Parc des Princes, Paris La Défense Arena, Accor Arena, Roland-Garros,
Stade Jean-Bouin, Adidas Arena, Zénith La Villette, Charléty, Paris Expo Porte de Versailles,
Parc des Expositions Villepinte, La Seine Musicale.

**Each venue is researched before it is written** (user decision): capacity confirmed against a
real source and recorded in `capacity_source`, and serving stations resolved to actual IDFM stop
identifiers by matching against the
[stations reference dataset](https://data.iledefrance-mobilites.fr/explore/dataset/emplacement-des-gares-idf/)
rather than by name. Name-matching will not survive contact with the validations file, whose
`LIBELLE_ARRET` values are uppercased and abbreviated.

Note that several venues have multiple configurations (Paris La Défense Arena seats materially
more for concerts than for rugby; Roland-Garros capacity is spread across courts). Record the
configuration the capacity refers to rather than collapsing it to one number.

### CLI

```
parismob fetch <source> [--year YYYY] [--all] [--dry-run]
parismob status                  # per-source partitions, rows, MB
parismob sql "SELECT ..."        # query the lake
parismob chart --station NAME    # the stop-zero payoff plot
```

Non-interactive, re-runnable, explicit about what it fetched.

---

## Verification

All steps below have been run. Results in [`FINDINGS.md`](FINDINGS.md).

1. `uv sync && uv run parismob --help` — package resolves, entrypoint works. **PASS**
2. `uv run parismob fetch calendar` — smallest source; confirms the write/manifest path
   end to end. **PASS** (143 holidays, 1,763 school-holiday rows, 340 for Zone C)
3. `uv run parismob fetch validations --year 2024` — confirms ZIP handling, tab parsing,
   the masked-value trap, and partition replacement. **PASS** (1,797,296 rows)
4. **Re-run step 3 verbatim.** Row count in `parismob status` must be identical. This is
   the idempotency test and the single most important check here. **PASS** — identical,
   and the archived bytes were reused rather than re-downloaded.
5. `uv run parismob fetch events` and `fetch validations --all`. **PASS** — 18,381,014
   validation rows across ten partitions.
6. **[corrected]** `parismob chart --station NAME` was replaced by
   `parismob events --venue <id>`. Name matching does not survive contact with the
   validations file, and the plan's own example string
   (`LA PLAINE STADE DE FRANCE`) does not exist — the real value is hyphenated.
   Venues resolve to numeric stop ids instead.
7. **The real acceptance test:** join validations against a handful of known Stade de France
   event dates and confirm the spike is statistically obvious. If it isn't, stop and debug
   before building anything on top.

   **[corrected] — see Decision 1.** Judged on percentile against a day-of-week
   baseline, not on a raw ratio. **PASS**: the four 2019 France home qualifiers land
   at the 96th–98th percentile of their year; the 2024 Olympic session days reach
   the 100th. Both Olympic opening ceremonies correctly score *below* baseline,
   because neither was held at this venue.

8. **[added] Date integrity.** `parismob sql` over the whole lake must show complete
   calendar coverage with no null dates. **PASS** — 3,653 days, 365/366 per year,
   zero nulls. This is the check that caught the 2024 three-date-format bug, which
   row counts alone would not have revealed.

## Out of scope here

eqasim, MATSim, OSRM, and any modelling.

**The real-time PRIM archiver is deliberately deferred to the next session** (user decision:
historical first). It is the highest-value next piece — every day it isn't running is a day of
data that can never be recovered — but it needs a PRIM API key that only you can register for,
and it's worth having the storage layer proven before something time-critical depends on it.

Register for the key at [prim.iledefrance-mobilites.fr](https://prim.iledefrance-mobilites.fr/)
while this work proceeds, so the archiver isn't blocked when we get to it.

**[corrected] The archiver is now written** (`sources/prim_realtime.py`), since the
storage layer it was waiting on is proven. It is the only collector here whose
endpoint contracts have *not* been checked against a live service, because that
needs the key. `parismob probe` verifies them the moment one exists; `parismob
fetch prim` then runs.

Note the tension this plan creates with its own argument. Decision 3 says the edge
is perishable data, and that a day PRIM is not running is lost permanently. The
historical ZIPs, by contrast, are static files that will be identical next year.
Sequencing the irreplaceable work after the replaceable work is backwards on the
plan's own reasoning — the key is the only thing standing between the project and
the data it says it most wants.
