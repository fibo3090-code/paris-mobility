"""Command line entrypoint. Non-interactive, re-runnable, explicit about what it did."""

from __future__ import annotations

import argparse
import sys

from rich.console import Console
from rich.table import Table

console = Console()


def _table(title: str, columns: list[str], rows: list[list[str]]) -> Table:
    t = Table(title=title, title_justify="left", header_style="bold")
    for c in columns:
        t.add_column(c)
    for r in rows:
        t.add_row(*r)
    return t


def _report(written) -> None:
    from .sources.base import Written

    if not written:
        console.print("[yellow]nothing written[/yellow]")
        return
    for w in written:
        assert isinstance(w, Written)
        loc = f" -> {w.path.name}" if w.path else ""
        console.print(f"[green]{w.source}[/green]/{w.partition}: [bold]{w.rows:,}[/bold] rows{loc}")
        for note in w.notes:
            console.print(f"    [dim]{note}[/dim]")


def cmd_fetch(args) -> int:
    from .sources import REGISTRY

    src = REGISTRY.get(args.source)
    if src is None:
        console.print(f"[red]unknown source {args.source!r}[/red]. Known: {', '.join(REGISTRY)}")
        return 2

    if args.dry_run:
        console.print(f"[cyan]dry run[/cyan] -- {args.source} would fetch:")
        if args.source == "validations":
            from .sources import idfm_validations

            available = idfm_validations.discover()
            targets = list(available) if args.all else ([args.year] if args.year else [])
            if not targets:
                console.print("  give --year YYYY or --all")
                return 2
            for y in targets:
                console.print(f"  {y}: {available.get(y, '[red]not published[/red]')}")
        else:
            console.print(f"  {args.source}: full refresh")
        return 0

    if args.source in ("validations", "surface", "accidents"):
        written = src.fetch(year=args.year, all_years=args.all, force=args.force)
    elif args.source == "road":
        written = src.fetch(
            year=args.year,
            month=args.month,
            reference=args.reference,
            force=args.force,
        )
    elif args.source == "rt-perimeter":
        written = src.fetch(file=args.file, force=args.force)
    elif args.source == "insee-od":
        written = src.fetch(year=args.year or "2021", force=args.force)
    elif args.source == "events":
        written = src.fetch()
    elif args.source == "prim":
        from .sources.prim_realtime import MissingKey

        try:
            written = src.fetch(
                disruptions_only=args.disruptions_only, per_stop=args.per_stop
            )
        except MissingKey as exc:
            console.print(f"[yellow]{exc}[/yellow]")
            return 3
    else:
        written = src.fetch()

    _report(written)
    return 0


def cmd_status(args) -> int:
    from .lake import lake_status, manifest

    rows = lake_status()
    if not rows:
        console.print("[yellow]lake is empty[/yellow] -- try `parismob fetch calendar`")
    else:
        console.print(
            _table(
                "lake",
                ["source", "partitions", "rows", "MB"],
                [[r["source"], str(r["partitions"]), f"{r['rows']:,}", f"{r['mb']:.1f}"] for r in rows],
            )
        )
        total = sum(r["rows"] for r in rows)
        size = sum(r["mb"] for r in rows)
        console.print(f"[bold]{total:,}[/bold] rows across {len(rows)} sources, {size:.1f} MB")

    m = manifest()
    if not m.is_empty():
        console.print(f"[dim]manifest: {m.height:,} fetches recorded[/dim]")
    return 0


def cmd_sql(args) -> int:
    from .lake import connect

    con = connect()
    try:
        cur = con.execute(args.query)
        columns = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
    except Exception as exc:
        console.print(f"[red]{type(exc).__name__}:[/red] {exc}")
        return 1
    finally:
        con.close()

    if not rows:
        console.print("[yellow]no rows[/yellow]")
        return 0
    console.print(
        _table("", columns, [[str(v) for v in row] for row in rows[: args.limit]])
    )
    if len(rows) > args.limit:
        console.print(f"[dim]showing {args.limit} of {len(rows):,} rows[/dim]")
    return 0


def cmd_venues(args) -> int:
    from .venues import capacities, load, resolve

    if args.resolve:
        console.print("[bold]recomputing serving stations from coordinates[/bold]")
        df = resolve()
        console.print(
            _table(
                "registry vs geography",
                ["venue", "recorded", "in radius", "missing from registry", "recorded but outside"],
                [
                    [
                        r["venue_id"],
                        str(r["recorded"]),
                        str(r["within_radius"]),
                        r["missing_from_registry"],
                        r["recorded_but_outside_radius"],
                    ]
                    for r in df.iter_rows(named=True)
                ],
            )
        )
        console.print(
            "[dim]'missing from registry' is informational: not every station within walking "
            "distance actually serves the venue.[/dim]"
        )
        return 0

    if args.capacities:
        df = capacities()
        console.print(
            _table(
                "capacities",
                ["venue", "config", "capacity", "unit", "confidence", "verified"],
                [
                    [
                        r["venue_id"],
                        r["config"],
                        f"{int(r['capacity']):,}",
                        r["unit"],
                        r["confidence"],
                        str(r["verified_on"]),
                    ]
                    for r in df.iter_rows(named=True)
                ],
            )
        )
        return 0

    console.print(
        _table(
            "venues",
            ["id", "name", "commune", "stations"],
            [
                [v.venue_id, v.name[:38], v.commune, " ".join(str(z) for z in v.serving_zdc)]
                for v in load()
            ],
        )
    )
    return 0


def cmd_events(args) -> int:
    """The acceptance test: score known event dates against their own weekday."""
    from .analysis import check_event_dates, outliers, venue_summary, verdict

    console.print(venue_summary(args.venue, args.years))
    console.print()

    if args.dates:
        scored = check_event_dates(args.venue, args.dates, args.years)
        if scored.is_empty():
            console.print("[yellow]none of those dates are in the lake[/yellow]")
            return 1
        console.print(
            _table(
                "known event dates, scored against their own weekday",
                ["date", "day", "validations", "dow median", "excess", "ratio", "pctile"],
                [
                    [
                        str(r["jour"]),
                        r["day"],
                        f"{int(r['validations']):,}",
                        f"{int(r['dow_median']):,}",
                        f"{int(r['excess']):+,}",
                        f"{r['ratio']:.2f}",
                        f"{r['pctile']:.1f}",
                    ]
                    for r in scored.iter_rows(named=True)
                ],
            )
        )
        console.print()
        console.print(verdict(scored))
        return 0

    df = outliers(args.venue, args.years, top=args.top)
    if df.is_empty():
        console.print("[yellow]no data[/yellow]")
        return 1
    console.print(
        _table(
            f"most anomalous days (day-of-week normalised), top {args.top}",
            ["date", "day", "validations", "dow median", "ratio"],
            [
                [
                    str(r["jour"]),
                    r["day"],
                    f"{int(r['validations']):,}",
                    f"{int(r['dow_median']):,}",
                    f"{r['ratio']:.2f}",
                ]
                for r in df.iter_rows(named=True)
            ],
        )
    )
    console.print(
        "[dim]Ranked against the same weekday, excluding Jul/Aug/Dec from the baseline. "
        "A raw ranking would surface commuter Mondays instead.[/dim]"
    )
    return 0


def cmd_probe(args) -> int:
    from .sources.prim_realtime import MissingKey, probe

    try:
        console.print(probe())
    except MissingKey as exc:
        console.print(f"[yellow]{exc}[/yellow]")
        return 3
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="parismob", description="Ile-de-France mobility data lake.")
    sub = p.add_subparsers(dest="command", required=True)

    f = sub.add_parser("fetch", help="collect a source into the lake")
    f.add_argument(
        "source",
        help=(
            "validations | surface | stations | referentiel | rt-perimeter | calendar | "
            "events | offre | qualite | multimodal | velo-counts | chantiers | transilien | "
            "road | velib | prim"
        ),
    )
    f.add_argument("--year", help="YYYY (validations, surface, road)")
    f.add_argument("--all", action="store_true", help="every year (validations, surface)")
    f.add_argument("--month", help="YYYY-MM (road: current rolling window)")
    f.add_argument("--reference", action="store_true", help="sensor geometry only (road)")
    f.add_argument("--force", action="store_true", help="re-download even if archived")
    f.add_argument("--dry-run", action="store_true", help="show what would be fetched")
    f.add_argument("--file", help="local CSV to import (rt-perimeter only)")
    f.add_argument("--disruptions-only", action="store_true", help="disruptions only (prim)")
    f.add_argument(
        "--per-stop",
        action="store_true",
        help="poll each watched stop instead of one network-wide snapshot (prim)",
    )
    f.set_defaults(func=cmd_fetch)

    s = sub.add_parser("status", help="per-source partitions, rows, size")
    s.set_defaults(func=cmd_status)

    q = sub.add_parser("sql", help="query the lake with DuckDB")
    q.add_argument("query")
    q.add_argument("--limit", type=int, default=25)
    q.set_defaults(func=cmd_sql)

    v = sub.add_parser("venues", help="the venue registry")
    v.add_argument("--resolve", action="store_true", help="recompute stations and report drift")
    v.add_argument("--capacities", action="store_true", help="show capacities with sources")
    v.set_defaults(func=cmd_venues)

    e = sub.add_parser("events", help="detect event days in the validations")
    e.add_argument("--venue", default="stade_de_france")
    e.add_argument("--dates", nargs="*", help="known event dates, YYYY-MM-DD (the acceptance test)")
    e.add_argument("--years", nargs="*", help="restrict to these partitions")
    e.add_argument("--top", type=int, default=15)
    e.set_defaults(func=cmd_events)

    pr = sub.add_parser("probe", help="check PRIM endpoint contracts against a live key")
    pr.set_defaults(func=cmd_probe)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        console.print("[yellow]interrupted[/yellow]")
        return 130
    except (FileNotFoundError, KeyError) as exc:
        console.print(f"[red]{exc}[/red]")
        return 1


if __name__ == "__main__":
    sys.exit(main())
