"""Event detection -- and the reason the obvious version of this test is wrong.

The naive check is: take a venue's stations, take known event dates, look for a
spike. Run it on 2019 international football at the Stade de France and it fails,
on a pipeline that is working perfectly.

Here is why. The four biggest validation days of 2019 at the Stade de France
stations were 14 Oct, 10 Sep, 14 Nov and 25 Mar -- France's four home Euro
qualifiers. The premise holds: events really are the top-ranked outliers. But they
came in at 1.50x the annual median, while an ordinary Tuesday reached 1.31x,
because La Plaine is a large office district and the weekday commuter baseline is
around 36,000 entries. A test asking "is the spike obvious" gets an ambiguous
answer and invites someone to go debugging correct code.

Normalise by day-of-week and the picture separates cleanly:

    weekend events   2.4 - 2.8x   unmistakable
    weekday events   1.20x        real, but buried under commuters

So: always compare a day against its own weekday, exclude July, August and
December from the baseline (summer collapse and the December 2019 strike would
drag it down), and check a weekend event first.

The second correction is magnitude. Validations count *entries* only. Somebody
attending a match taps in at their origin station on the way there and only passes
a stadium gate going home, and the homeward flow disperses across four stations
plus buses, walking and cars. Measured across all four serving stations, a ~70-80k
attendance produces roughly +16,000 marginal validations -- about 20% of
attendance, not 100%. That ratio is stable enough to model with. It is only a
problem if you went in expecting 80,000 and concluded the data was broken.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

from .config import LAKE_DIR
from .venues import serving_zdc, venue

#: Months excluded from the day-of-week baseline. July and August are the summer
#: collapse; December carries both the holidays and, in 2019, a month-long strike.
BASELINE_EXCLUDED_MONTHS = (7, 8, 12)


def daily_series(zdc: list[int], years: list[str] | None = None) -> pl.DataFrame:
    """Total daily validations across a set of stations.

    Masked counts (`"Moins de 5"`) are treated as absent rather than zero. At a
    venue station the effect is immaterial -- they occur at quiet stops -- but the
    choice is made here, in the open, instead of by a silent cast upstream.
    """
    part_dir = LAKE_DIR / "idfm_validations"
    if not part_dir.exists() or not any(part_dir.glob("*.parquet")):
        raise FileNotFoundError("no validations in the lake -- run `parismob fetch validations --all`")

    files = sorted(part_dir.glob("*.parquet"))
    if years:
        keep = set(years)
        files = [f for f in files if f.stem in keep]
        if not files:
            raise FileNotFoundError(f"no partitions for {years}")

    return (
        pl.scan_parquet(files)
        .filter(pl.col("id_refa_lda").is_in(zdc) & pl.col("nb_vald").is_not_null())
        .group_by("jour")
        .agg(pl.col("nb_vald").sum().alias("validations"))
        .sort("jour")
        .collect()
    )


def with_dow_baseline(series: pl.DataFrame) -> pl.DataFrame:
    """Attach each day's day-of-week median and the ratio against it.

    The baseline is a median, not a mean, so that the event days being measured do
    not inflate the yardstick they are measured against.
    """
    if series.is_empty():
        return series

    df = series.with_columns(
        pl.col("jour").dt.weekday().alias("dow"),
        pl.col("jour").dt.month().alias("month"),
        pl.col("jour").dt.year().alias("year"),
    )
    baseline = (
        df.filter(~pl.col("month").is_in(list(BASELINE_EXCLUDED_MONTHS)))
        .group_by(["year", "dow"])
        .agg(pl.col("validations").median().alias("dow_median"))
    )
    return (
        df.join(baseline, on=["year", "dow"], how="left")
        .with_columns((pl.col("validations") / pl.col("dow_median")).alias("ratio"))
        .sort("jour")
    )


def outliers(venue_id: str, years: list[str] | None = None, top: int = 15) -> pl.DataFrame:
    """The most anomalous days at a venue, ranked by day-of-week-normalised ratio."""
    df = with_dow_baseline(daily_series(list(serving_zdc(venue_id)), years))
    if df.is_empty():
        return df
    return (
        df.filter(pl.col("ratio").is_not_null())
        .sort("ratio", descending=True)
        .head(top)
        .select("jour", "validations", "dow_median", "ratio")
        .with_columns(
            pl.col("jour").dt.to_string("%a").alias("day"),
            pl.col("ratio").round(2),
            pl.col("dow_median").round(0),
        )
    )


def check_event_dates(venue_id: str, dates: list[str], years: list[str] | None = None) -> pl.DataFrame:
    """Score known event dates against their own weekday. The acceptance test.

    `ratio` is the day against its day-of-week median; `excess` is the marginal
    validations, which is the figure to compare against attendance. `pctile` is
    where the day sits in that year's distribution -- the honest measure of
    "obvious", and the one that survives the weekday/weekend split.
    """
    df = with_dow_baseline(daily_series(list(serving_zdc(venue_id)), years))
    if df.is_empty():
        return df

    wanted = [dt.date.fromisoformat(d) for d in dates]
    ranked = df.filter(pl.col("ratio").is_not_null()).with_columns(
        (pl.col("ratio").rank("average") / pl.len() * 100).round(1).alias("pctile")
    )
    return (
        ranked.filter(pl.col("jour").is_in(wanted))
        .with_columns(
            (pl.col("validations") - pl.col("dow_median")).round(0).alias("excess"),
            pl.col("jour").dt.to_string("%a").alias("day"),
            pl.col("ratio").round(2),
            pl.col("dow_median").round(0),
        )
        .select("jour", "day", "validations", "dow_median", "excess", "ratio", "pctile")
        .sort("jour")
    )


def verdict(scored: pl.DataFrame) -> str:
    """Turn the scored dates into a pass/fail the CLI can print.

    Deliberately judged on percentile rather than raw ratio: a weekday event at
    1.20x and a weekend event at 2.4x are both genuine detections, and only a
    distribution-relative measure treats them as such.
    """
    if scored.is_empty():
        return "NO DATA -- no matching dates in the lake."

    median_pct = float(scored["pctile"].median())
    top_decile = int((scored["pctile"] >= 90).sum())
    n = scored.height

    lines = [
        f"{n} event dates checked.",
        f"median percentile {median_pct:.1f}, {top_decile}/{n} in the top decile of their year.",
    ]
    if median_pct >= 90:
        lines.append("PASS -- events sit firmly in the tail. The join and the parser are sound.")
    elif median_pct >= 75:
        lines.append(
            "PASS (weak) -- detectable but not dominant. Expected for weekday events at a "
            "station with a heavy commuter baseline; check a weekend event before suspecting a bug."
        )
    else:
        lines.append(
            "FAIL -- events are not in the tail. Check the ZDC join first (are these the right "
            "stations?), then the date parse (DD/MM/YYYY), before doubting the premise."
        )
    return "\n".join(lines)


def venue_summary(venue_id: str, years: list[str] | None = None) -> str:
    v = venue(venue_id)
    df = daily_series(list(v.serving_zdc), years)
    if df.is_empty():
        return f"{v.name}: no validations found for ZDC {v.serving_zdc}"
    return (
        f"{v.name} ({v.commune})\n"
        f"  stations : {', '.join(str(z) for z in v.serving_zdc)}\n"
        f"  days     : {df.height:,} from {df['jour'].min()} to {df['jour'].max()}\n"
        f"  median   : {int(df['validations'].median()):,} validations/day\n"
        f"  peak     : {int(df['validations'].max()):,}"
    )
