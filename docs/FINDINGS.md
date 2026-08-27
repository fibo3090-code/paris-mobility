# What contact with the data changed

`PLAN.md` was written before anything had been downloaded. This records what was
wrong with it, what turned out to be true, and what only became visible once
18.4 million rows were on disk. Everything below is reproducible from the CLI.

## The plan's factual errors

| Claim | Reality |
|---|---|
| ZIPs listed in the dataset's `/attachments` | That endpoint holds one PDF. The ZIPs are file-typed fields inside the dataset's ten *records*. |
| Stop id column is `ID_ZDC` | Three names across the years: `ID_REFA_LDA`, `lda`, `ID_ZDC`. All carry the same value. |
| "Tab-separated `.txt`, not CSV" | True for most years. 2015 and 2022-S2 are semicolon-separated, and 2015 uses `.csv`. |
| `NB_VALD` contains `"Moins de 5"` | True for 2015-2017 only (116k, 116k, 54k rows). Zero masked values from 2018 on. |
| Station is `LA PLAINE STADE DE FRANCE` | It is `LA PLAINE-STADE DE FRANCE`, hyphenated. The plan's own verification command would have matched nothing. |
| Stade de France "puts ~80k onto RER B and D" | A weekday match yields roughly **+10,000 to +13,000** marginal validations across four stations. See *Capture ratio* below. |
| ~123 MB text per year, ~20M rows total | 18.4M rows. Compresses to **77 MB of Parquet for the entire decade.** |

## Format drift the plan did not anticipate

Ten independent inconsistencies across ten years. Each is handled explicitly in
`sources/idfm_validations.py`; none can be assumed away.

1. **Separator** — `;` in 2015 and 2022-S2, tab elsewhere.
2. **Extension** — `.csv` in 2015, `.txt` after.
3. **Stop id column** — `ID_REFA_LDA` / `lda` / `ID_ZDC`.
4. **Encoding** — 2024-S1 is latin-1 while 2024-T3/T4 are utf-8, in the same archive.
5. **UTF-16** — 2023-S1 is UTF-16 with a byte-order mark.
6. **UTF-8 BOM** — 2022-S2, which otherwise rides along inside the first column name.
7. **Period naming** — semesters and quarters coexist (2024 has S1, T3, T4).
8. **Nested archives** — the 2020 ZIP contains copies of the 2015-2019 ZIPs.
9. **Stray whitespace** — `2023_S1_NB_FER .txt`, with a space before the extension.
10. **Date format** — `DD/MM/YYYY`, `DD/MM/YY` *and* `YYYY-MM-DD`, all three inside 2024.

Two of these were silent rather than loud, which is the dangerous kind:

- **The nested 2020 archives** would have folded five years of data into the 2020
  partition. Row counts would have looked plausible.
- **The 2024 dates** were worse. `%d/%m/%Y` parses `01/07/24` as the year **24**
  without complaint, and returns null for `2024-10-01`. The first run produced
  Olympic results dated `0024-08-04` and silently dropped Q4. Both now fail loudly:
  the parser rejects any member whose dates fall outside 2000-2100 or whose parse
  failure rate exceeds 1%.

The stray-space bug is the cautionary one. It cost **half of 2023** — 849,596 rows
instead of 1,945,805 — and announced itself only as a one-line "skipped
unrecognised member" note. Without per-year row counts to compare against, it
would have passed for real.

## The acceptance test in the plan is wrong

`PLAN.md` step 7 said: join known Stade de France event dates, confirm the spike is
"statistically obvious", and **stop and debug if it isn't**.

Run that on 2019 international football and it is not obvious. The four France
home Euro qualifiers were the four biggest days of the year at those stations —
the premise holds perfectly — but they land at 1.20-1.27x the annual median while
an ordinary Tuesday reaches 1.31x. La Plaine is a large office district; the
weekday commuter baseline is ~47,000 entries. A correct pipeline fails its own gate.

**Three corrections, all implemented in `analysis.py`:**

1. **Normalise by day-of-week.** Compare a day to its own weekday, not to the year.
2. **Exclude July, August and December from the baseline** — summer collapse and,
   in 2019, a month-long strike.
3. **Judge on percentile, not ratio.** A weekday event at 1.20x and a weekend event
   at 4.85x are both genuine detections; only a distribution-relative measure
   treats them as such.

With those, the same four dates pass unambiguously:

```
2019-03-25  Mon  57,072   +9,634   1.20x   96.2 pctile
2019-09-10  Tue  60,311  +10,479   1.21x   96.7 pctile
2019-10-14  Mon  60,426  +12,988   1.27x   98.4 pctile
2019-11-14  Thu  60,325  +11,377   1.23x   97.5 pctile
-> median percentile 97.1, 4/4 in the top decile. PASS
```

## Capture ratio is not a constant

Validations count **entries only**. Somebody attending a match taps in at their
origin on the way there and only passes a stadium gate going home, and that
homeward flow disperses across several stations plus buses, walking and cars. The
fraction of attendance that shows up as marginal validations is therefore well
below 1 — and it moves a great deal:

| Occasion | Excess validations | Approx. attendance | Capture |
|---|---|---|---|
| Weekday international football (2019) | +10,000 to +13,000 | ~75,000 | ~15% |
| Olympic session day (2024-08-04) | +71,948 | ~77,000 | ~90% |

The Olympic figure is high because the commuter baseline was itself suppressed
(residents away or working from home), sessions ran morning and evening, and the
crowd was visitors rather than locals with cars. **Anyone modelling this must treat
capture as a function of event type and day type, not a fixed coefficient.**

## The model detects absence correctly

A useful negative control fell out of the 2024 test. Two dates scored *below*
their weekday baseline, at the 10th percentile:

```
2024-07-26  Fri  26,689   -5,578   0.83x   10.9 pctile   Olympic opening ceremony
2024-08-28  Wed  32,634   -7,396   0.82x   10.2 pctile   Paralympic opening ceremony
```

Both are correct. Neither ceremony was held at the Stade de France — they were on
the Seine and at Place de la Concorde — and the security perimeter suppressed
ordinary traffic. The pipeline is not merely finding spikes wherever it is told to
look.

## Signal quality varies by venue, sharply

Not every venue is equally learnable. Measured on 2023:

| Venue | Best ratio | Why |
|---|---|---|
| Villepinte Expo | **13.8x** | One dedicated RER B station, baseline ~1,000/day |
| Parc des Princes | 2.1x | Suburban metro stops, moderate baseline |
| Stade de France | 1.2-6.4x | Depends entirely on weekday vs weekend |
| Accor Arena | 1.5x | Gare de Lyon and Gare de Bercy baselines swamp it |

Venue selection matters more than model choice. Start with Villepinte and the
weekend Stade de France events; Accor Arena is close to unusable on entries alone.

## La météo : forte sur le vélo, négligeable sur le ferré

Données Météo-France quotidiennes, huit départements franciliens, 357 postes,
1950-2026. Testée dès l'intégration, elle sépare nettement deux régimes.

**Sur le vélo, la pluie est un déterminant de premier ordre** — relation monotone
sur les quatre tranches :

| Pluie (mm/jour) | Jours | Vélos/jour |
|---|---|---|
| sec | 165 | 222 579 |
| 0–2 | 126 | 217 269 |
| 2–8 | 78 | 196 658 |
| > 8 | 26 | **161 776** (−27 %) |

**Sur le ferré, l'effet brut est un piège.** La comparaison directe suggère que la
pluie modérée *augmente* la fréquentation (5,90 M contre 5,43 M par temps sec).
C'est faux : les jours secs se concentrent en été, quand la fréquentation est
basse pour d'autres raisons. En normalisant chaque jour par la médiane de son mois,
l'effet s'effondre :

| Pluie | Ratio vs médiane du mois |
|---|---|
| sec | 0,966 |
| 0–2 mm | 0,967 |
| 2–8 mm | 0,981 |
| > 8 mm | 0,948 |

Environ ±2 %, non monotone. **L'effet apparent était de la saisonnalité pure.**

Conséquence pratique : une variable météo brute sur les validations ferrées
capture surtout le mois. Elle doit être introduite en résiduel d'un modèle qui
contrôle déjà la saison, sinon elle paraîtra significative en ne mesurant rien.
C'est le même piège que le jour de semaine sur la détection d'événements, un cran
plus subtil.

## Still unverified

- **PRIM real-time** — the collector is written but its endpoint contracts have not
  been exercised, because that needs an API key only the operator can register for.
  `parismob probe` checks them the moment a key exists. Everything else in this
  repo was verified against live data.
- **eqasim / EGT access** — the plan treats the synthetic population as solved. The
  Ile-de-France household travel survey has historically required a data agreement
  rather than being an open download. Worth confirming before it becomes a
  structural assumption.
- **2025 onward** — the historical archive stops at 2024. 2025 exists only in
  separate quarterly datasets whose fields are lowercased and renamed again
  (`ida` rather than `ID_REFA_LDA`). Not yet collected.
- **2019-03-30/31** is the strongest non-Olympic weekend outlier in the data
  (2.20x) and is currently unexplained. A good first test of the event registry.
