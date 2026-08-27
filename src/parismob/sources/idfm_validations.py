"""IDFM rail validations, 2015-2024 -- the label set this project is built on.

Access path
-----------
The ten yearly ZIPs are file-typed fields inside the *records* of
`histo-validations-reseau-ferre`, not dataset attachments. The `/attachments`
endpoint returns a single PDF and nothing else; a collector pointed there finds
no data and reports success.

Format drift
------------
Every one of the following was observed in the real archives, not anticipated.
The parser handles each explicitly rather than assuming a stable schema:

* Separator      -- `;` in 2015 and 2022-S2, TAB everywhere else.
* Extension      -- `.csv` in 2015, `.txt` from 2016 on.
* Stop id column -- `ID_REFA_LDA`, `lda` (lowercase), or `ID_ZDC` depending on year.
* Encoding       -- 2024-S1 is latin-1 while 2024-T3/T4 are utf-8. Same year.
* BOM            -- 2022-S2 carries a UTF-8 BOM that becomes part of column one.
* Period naming  -- semesters (`S1`/`S2`) and quarters (`T3`/`T4`) coexist in 2024.
* Nested ZIPs    -- the 2020 archive contains copies of the 2015-2019 archives.
                    Recursing into them double-counts five years into one partition.
* Decimal comma  -- profile percentages are French-formatted (`1,71`).

Masked counts
-------------
`NB_VALD` is not natively numeric: small counts are published as strings such as
`"Moins de 5"` to prevent re-identification. Coercing those to 0 understates
quiet stations and coercing to 5 invents data, so all three facts are kept:
the original string, a nullable integer, and a boolean flag. Aggregations then
make their own choice explicitly rather than inheriting a silent one.
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import polars as pl

from ..lake import write_partition
from .base import Fetched, Written, client, download, ods_records

name = "idfm_validations"

BASE = "https://data.iledefrance-mobilites.fr"
DATASET = "histo-validations-reseau-ferre"

# Any of these name the stop-place identifier. Despite the `LDA` and `ZDC`
# labels the values are the same series: they join to `id_ref_zdc` in
# `emplacement-des-gares-idf` (verified: 71379 -> Porte Maillot, RER C/E + M1).
_STOP_ID_COLUMNS = {"id_refa_lda", "lda", "id_zdc", "ida"}

_PCT_COLUMNS = {"pourc_validations", "pourcentage_validations"}

# `2019_S1_NB_FER.txt`, `2015S1_NB_FER.csv`, `2024_T3_PROFIL_FER.txt`, and
# `2023_S1_NB_FER .txt` -- that stray space is real and cost half of 2023 until
# the pattern tolerated it. Whitespace is allowed anywhere a separator appears.
#
# The surface network (bus and tram) uses the same archive layout with `SURFACE`
# in place of `FER`, so the network token is captured rather than hard-coded. It
# is returned to the caller: the two networks have different column sets and must
# not be concatenated into one partition just because they parse alike.
_MEMBER = re.compile(
    r"(?P<year>\d{4})\s*_?\s*(?P<period>[ST]\d)\s*_\s*(?P<kind>NB|PROFIL)\s*_\s*"
    r"(?P<network>FER|SURFACE)\s*\.(txt|csv)$",
    re.I,
)


def discover() -> dict[str, str]:
    """Map year -> ZIP url, straight from the dataset records."""
    records = ods_records(DATASET, base=BASE, select="annee,reseau_ferre")
    out: dict[str, str] = {}
    for r in records:
        f = r.get("reseau_ferre") or {}
        url = f.get("url")
        year = str(r.get("annee") or "").strip()
        if url and year:
            out[year] = url
    return dict(sorted(out.items()))


def _decode(raw: bytes) -> str:
    """Decode a member, tolerating the per-file encoding drift.

    Byte-order marks are checked *first* and explicitly. That ordering is not
    cosmetic: `latin-1` decodes any byte sequence without ever raising, so a
    UTF-16 file reaching it comes back as null-interleaved mojibake -- column
    `jour` becomes `j\\x00o\\x00u\\x00r\\x00` -- and every downstream lookup fails
    for a reason that looks nothing like the cause. At least one year in this
    archive is UTF-16, so this is a real path, not a defensive one.

    BOM-less UTF-16 is caught by the null-byte heuristic: French text in any
    8-bit codec contains no NULs, so a NUL in the first kilobyte means wide chars.
    """
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig")
    if b"\x00" in raw[:1024]:
        # No BOM but wide characters. Guess endianness from where the NULs land.
        enc = "utf-16-le" if raw[1:2] == b"\x00" else "utf-16-be"
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            pass
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


#: Columns every daily-count member must have after canonicalisation. Checked so a
#: mis-decode or a future rename fails at the parse, not silently in the lake.
#:
#: Only the genuinely universal columns are required. The rail files key on a stop
#: (`id_refa_lda`); the surface files key on a line, because a bus is validated on
#: board rather than at a station entrance. Demanding the rail columns of a surface
#: file would reject a perfectly good archive.
_REQUIRED_COUNTS = {"jour", "nb_vald"}
_REQUIRED_COUNTS_FER = _REQUIRED_COUNTS | {"libelle_arret", "id_refa_lda"}
_REQUIRED_PROFILES = {"cat_jour", "trnc_horr_60", "pourc_validations"}


def _require(df: pl.DataFrame, needed: set[str], member: str) -> None:
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(
            f"{member}: missing {sorted(missing)} after canonicalisation. "
            f"Got {df.columns}. Either the encoding was guessed wrong or IDFM "
            f"renamed a column -- do not write this partition until it is resolved."
        )


def _sniff_separator(header: str) -> str:
    return ";" if header.count(";") > header.count("\t") else "\t"


def _canonical(col: str) -> str:
    c = col.strip().lstrip("﻿").lower()
    if c in _STOP_ID_COLUMNS:
        return "id_refa_lda"
    if c in _PCT_COLUMNS:
        return "pourc_validations"
    return c


def _read_member(raw: bytes) -> pl.DataFrame:
    """Parse one member to all-Utf8 columns with canonical names.

    Everything is read as text and cast afterwards on purpose: type inference
    across a file whose count column mixes integers and `"Moins de 5"` produces a
    different schema per year, and silently nulls the masked rows in the process.
    """
    text = _decode(raw)
    first = text.split("\n", 1)[0]
    df = pl.read_csv(
        io.BytesIO(text.encode("utf-8")),
        separator=_sniff_separator(first),
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )
    return df.rename({c: _canonical(c) for c in df.columns})


#: Date formats seen in the archive. 2024 alone uses all three -- `01/01/2024` in
#: S1, `01/07/24` in T3, `2024-10-01` in T4 -- so this is per-member, not per-year.
#: The two-digit form is the dangerous one: `%d/%m/%Y` parses `01/07/24` as the
#: year 24 without complaint, putting the row two millennia in the past instead of
#: rejecting it. The ISO form simply yields null under the same format string,
#: which silently drops a quarter of the year from any date-filtered query.
_DATE_FORMATS = ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d")

#: A member whose dates fail to parse beyond this fraction is a format change, not
#: dirt. Refuse the partition rather than write one with a hole in it.
_MAX_UNPARSED_DATES = 0.01


def _parse_date(col: str) -> pl.Expr:
    """Pick the format from the string's shape, rather than trying each in turn.

    Trying formats in order and taking the first success does not work here:
    `%d/%m/%Y` happily consumes `01/07/24` and returns the year 24, so it wins
    before the two-digit format is ever reached. Matching the shape first makes
    the choice unambiguous and keeps a malformed value as null instead of as a
    plausible-looking wrong date.
    """
    s = pl.col(col).str.strip_chars()
    return (
        pl.when(s.str.contains(r"^\d{4}-\d{2}-\d{2}"))
        .then(s.str.to_date("%Y-%m-%d", strict=False))
        .when(s.str.contains(r"^\d{1,2}/\d{1,2}/\d{4}$"))
        .then(s.str.to_date("%d/%m/%Y", strict=False))
        .when(s.str.contains(r"^\d{1,2}/\d{1,2}/\d{2}$"))
        .then(s.str.to_date("%d/%m/%y", strict=False))
        .otherwise(None)
        .alias(col)
    )


def _check_dates(df: pl.DataFrame, member: str) -> None:
    if df.is_empty():
        return
    bad = int(df["jour"].is_null().sum())
    rate = bad / df.height
    if rate > _MAX_UNPARSED_DATES:
        raise ValueError(
            f"{member}: {bad:,} of {df.height:,} dates ({rate:.1%}) match none of "
            f"{_DATE_FORMATS}. IDFM has changed the date format again -- add it "
            f"before writing, or the partition lands with a hole in it."
        )
    # A year far from the one the filename claims means a two-digit year was read
    # as a four-digit one. Cheap to check, and invisible downstream if missed.
    years = df["jour"].dt.year().drop_nulls()
    if years.len() and (int(years.min()) < 2000 or int(years.max()) > 2100):
        raise ValueError(
            f"{member}: parsed dates span {years.min()}-{years.max()}, which is not "
            f"a plausible calendar year. A two-digit year was almost certainly read literally."
        )


def _finalise_counts(df: pl.DataFrame, year: str, period: str) -> pl.DataFrame:
    """Type the daily-count table and preserve the masked values."""
    numeric = pl.col("nb_vald").str.strip_chars().str.replace_all(r"\s", "")
    exprs = [
        _parse_date("jour"),
        pl.col("nb_vald").alias("nb_vald_raw"),
        numeric.cast(pl.Int64, strict=False).alias("nb_vald_num"),
    ]
    # Rail-only columns. The surface archive keys on the line instead, so these are
    # applied when present rather than assumed.
    if "id_refa_lda" in df.columns:
        exprs.append(pl.col("id_refa_lda").cast(pl.Int64, strict=False).alias("id_refa_lda"))
    if "categorie_titre" in df.columns:
        # '?' is IDFM's placeholder for an unrecorded fare category.
        exprs.append(
            pl.when(pl.col("categorie_titre").str.strip_chars() == "?")
            .then(None)
            .otherwise(pl.col("categorie_titre").str.strip_chars())
            .alias("categorie_titre")
        )
    df = df.with_columns(exprs)
    return df.with_columns(
        pl.col("nb_vald_num").is_null().alias("nb_vald_masked"),
        pl.lit(year).alias("annee"),
        pl.lit(period).alias("periode"),
    ).drop("nb_vald").rename({"nb_vald_num": "nb_vald"})


def _finalise_profiles(df: pl.DataFrame, year: str, period: str) -> pl.DataFrame:
    """Type the hourly-profile table, converting the French decimal comma."""
    exprs = [
        pl.col("pourc_validations")
        .str.strip_chars()
        .str.replace(",", ".", literal=True)
        .cast(pl.Float64, strict=False)
        .alias("pourc_validations"),
        pl.lit(year).alias("annee"),
        pl.lit(period).alias("periode"),
    ]
    if "id_refa_lda" in df.columns:
        exprs.insert(0, pl.col("id_refa_lda").cast(pl.Int64, strict=False).alias("id_refa_lda"))
    return df.with_columns(exprs)


def parse_zip(archive: Path | bytes, year: str) -> tuple[pl.DataFrame, pl.DataFrame, list[str]]:
    """Return (daily counts, hourly profiles, notes) for one year's archive.

    Members belonging to a year other than the one requested are skipped. This is
    what stops the nested 2015-2019 archives inside the 2020 ZIP from being folded
    into the 2020 partition -- and nested ZIPs are never recursed into at all,
    since those years are fetched from their own records.
    """
    src = io.BytesIO(archive) if isinstance(archive, bytes) else archive
    counts: list[pl.DataFrame] = []
    profiles: list[pl.DataFrame] = []
    notes: list[str] = []

    with zipfile.ZipFile(src) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            base = Path(info.filename).name
            if base.lower().endswith(".zip"):
                notes.append(f"skipped nested archive {base} (fetched under its own year)")
                continue
            m = _MEMBER.search(base)
            if not m:
                notes.append(f"skipped unrecognised member {base}")
                continue
            if m.group("year") != year:
                notes.append(f"skipped {base}: belongs to {m.group('year')}, not {year}")
                continue

            period = m.group("period").upper()
            network = m.group("network").upper()
            df = _read_member(z.read(info))
            if m.group("kind").upper() == "NB":
                _require(
                    df,
                    _REQUIRED_COUNTS_FER if network == "FER" else _REQUIRED_COUNTS,
                    base,
                )
                typed = _finalise_counts(df, year, period)
                _check_dates(typed, base)
                counts.append(typed)
            else:
                _require(df, _REQUIRED_PROFILES, base)
                profiles.append(_finalise_profiles(df, year, period))

    count_df = pl.concat(counts, how="diagonal_relaxed") if counts else pl.DataFrame()
    profile_df = pl.concat(profiles, how="diagonal_relaxed") if profiles else pl.DataFrame()
    return count_df, profile_df, notes


def fetch(year: str | None = None, all_years: bool = False, force: bool = False) -> list[Written]:
    """Collect one year, or every year. Re-running replaces partitions in place."""
    available = discover()
    if all_years:
        years = list(available)
    elif year:
        if year not in available:
            raise SystemExit(f"{year} not published. Available: {', '.join(available)}")
        years = [year]
    else:
        raise SystemExit("give --year YYYY or --all")

    written: list[Written] = []
    with client() as cli:
        for y in years:
            got: Fetched = download(
                available[y],
                source=name,
                filename=f"data-rf-{y}.zip",
                force=force,
                note=f"rail validations {y}",
                cli=cli,
            )
            counts, profiles, notes = parse_zip(got.data, y)
            if got.from_cache:
                notes.insert(0, "raw bytes reused from archive; no download")

            if not counts.is_empty():
                path = write_partition(counts, name, y)
                masked = int(counts["nb_vald_masked"].sum())
                if masked:
                    notes.append(f"{masked:,} masked counts preserved as null + flag")
                written.append(Written(name, y, counts.height, path, notes))
            if not profiles.is_empty():
                p_path = write_partition(profiles, f"{name}_profil", y)
                written.append(Written(f"{name}_profil", y, profiles.height, p_path, []))
    return written
