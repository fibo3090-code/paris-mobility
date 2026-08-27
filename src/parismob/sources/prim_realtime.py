"""IDFM PRIM real-time archiver -- the only source here that cannot be backfilled.

Everything else in this repo is a static file that will still be downloadable next
year. This one is not. PRIM publishes the live state of the network and keeps no
public history, so a day not captured is a day gone permanently. That asymmetry is
the entire argument for the project owning a collector at all, which is why this
runs on a schedule rather than on demand.

What it captures
----------------
`disruptions`  -- every active disruption message on the network, per poll.
`departures`   -- SIRI StopMonitoring for a watched set of stops: scheduled versus
                  expected passing times. Differencing those two over months is
                  what yields planned-versus-actual service by line by minute,
                  which is the record that cannot be bought.

Snapshot semantics
------------------
Each poll is its own partition, keyed by UTC timestamp, and nothing ever rewrites
an earlier one. That is deliberate: the point of the archive is what was *being
said at the time*, including the parts later corrected.

Quota
-----
PRIM allows on the order of 20,000 requests/day. One disruption poll plus one
departure poll per watched stop every five minutes is roughly
`288 * (1 + len(stops))` requests/day, so about 60 stops fits comfortably.
`--every` and the watch list are the two dials.

Authentication
--------------
Register at https://prim.iledefrance-mobilites.fr/ and export the key as
`PRIM_API_KEY`. The key is read from the environment and never written to the
manifest or to disk.

The response shapes below follow IDFM's published SIRI-Lite contract, but unlike
every other collector here they were **not** verified against a live endpoint --
that needs a key only the operator can register for. `--probe` exists to check
them the moment a key is available, and the parser stores the raw JSON
regardless, so a shape surprise costs a re-parse and never a lost day of data.
"""

from __future__ import annotations

import datetime as dt
import json
import os

import polars as pl

from ..lake import raw_path, record_fetch, sha256_bytes, write_partition
from .base import Written, client

name = "prim_realtime"

DISRUPTIONS_URL = "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2"
STOP_MONITORING_URL = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring"

#: SIRI EstimatedTimetable: the whole network in a single exchange. Confirmed on
#: the IDFM catalogue as "Prochains passages -- requete globale". This is the
#: endpoint that makes the archive viable: polling stops individually costs one
#: request each, so watching 60 stops every five minutes is ~17,000 requests/day
#: against a ~20,000 quota. One global call per poll costs 288/day at the same
#: cadence, or 1,440/day at one-minute resolution -- and covers every line rather
#: than a hand-picked subset.
ESTIMATED_TIMETABLE_URL = "https://prim.iledefrance-mobilites.fr/marketplace/estimated-timetable"

ENV_KEY = "PRIM_API_KEY"


class MissingKey(RuntimeError):
    pass


def api_key() -> str:
    key = os.environ.get(ENV_KEY, "").strip()
    if not key:
        raise MissingKey(
            f"{ENV_KEY} is not set. Register at https://prim.iledefrance-mobilites.fr/ "
            f"and export the key. Until then this source cannot run -- and every day "
            f"it does not run is a day of data that cannot be recovered."
        )
    return key


def _stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _archive(payload: bytes, filename: str, url: str, note: str, rows: int | None) -> None:
    dest = raw_path(name, filename)
    dest.write_bytes(payload)
    record_fetch(
        source=name,
        url=url,  # never carries the key: it travels in a header
        path=dest,
        n_bytes=len(payload),
        digest=sha256_bytes(payload),
        rows=rows,
        note=note,
    )


def poll_disruptions(key: str, cli) -> list[Written]:
    """One snapshot of every active disruption message."""
    stamp = _stamp()
    resp = cli.get(DISRUPTIONS_URL, headers={"apiKey": key})
    resp.raise_for_status()
    payload = resp.content
    body = json.loads(payload)

    disruptions = body.get("disruptions", body if isinstance(body, list) else [])
    _archive(payload, f"disruptions-{stamp}.json", DISRUPTIONS_URL, "disruption snapshot", len(disruptions))

    rows = []
    for d in disruptions:
        periods = d.get("applicationPeriods") or [{}]
        messages = d.get("messages") or []
        rows.append(
            {
                "polled_at": stamp,
                "disruption_id": d.get("id"),
                "status": d.get("status"),
                "severity": (d.get("severity") or {}).get("name"),
                "cause": d.get("cause"),
                "title": (messages[0].get("title") if messages else None),
                "text": (messages[0].get("text") if messages else None),
                "begin": periods[0].get("begin"),
                "end": periods[0].get("end"),
                "last_update": d.get("lastUpdate"),
                "raw": json.dumps(d, ensure_ascii=False),
            }
        )

    if not rows:
        return [Written(f"{name}_disruptions", stamp, 0, None, ["no active disruptions"])]

    df = pl.DataFrame(rows, infer_schema_length=None)
    path = write_partition(df, f"{name}_disruptions", stamp)
    return [Written(f"{name}_disruptions", stamp, df.height, path, [])]


def poll_departures(key: str, stops: list[str], cli) -> list[Written]:
    """SIRI StopMonitoring for each watched stop: aimed versus expected times."""
    stamp = _stamp()
    rows = []
    notes: list[str] = []

    for stop in stops:
        try:
            resp = cli.get(
                STOP_MONITORING_URL,
                params={"MonitoringRef": stop},
                headers={"apiKey": key},
            )
            resp.raise_for_status()
        except Exception as exc:  # one bad stop must not abandon the whole poll
            notes.append(f"{stop}: {type(exc).__name__} {exc}")
            continue

        payload = resp.content
        _archive(
            payload,
            f"departures-{stop.replace(':', '_')}-{stamp}.json",
            STOP_MONITORING_URL,
            f"stop monitoring {stop}",
            None,
        )

        body = json.loads(payload)
        deliveries = (
            body.get("Siri", {})
            .get("ServiceDelivery", {})
            .get("StopMonitoringDelivery", [])
        )
        for delivery in deliveries:
            for visit in delivery.get("MonitoredStopVisit", []):
                journey = visit.get("MonitoredVehicleJourney", {})
                call = journey.get("MonitoredCall", {})
                rows.append(
                    {
                        "polled_at": stamp,
                        "monitoring_ref": stop,
                        "line_ref": journey.get("LineRef", {}).get("value"),
                        "destination": (journey.get("DestinationName") or [{}])[0].get("value"),
                        "vehicle_journey": journey.get("FramedVehicleJourneyRef", {}).get(
                            "DatedVehicleJourneyRef"
                        ),
                        "aimed_departure": call.get("AimedDepartureTime"),
                        "expected_departure": call.get("ExpectedDepartureTime"),
                        "aimed_arrival": call.get("AimedArrivalTime"),
                        "expected_arrival": call.get("ExpectedArrivalTime"),
                        "departure_status": call.get("DepartureStatus"),
                        "at_stop": call.get("VehicleAtStop"),
                    }
                )

    if not rows:
        return [Written(f"{name}_departures", stamp, 0, None, notes or ["no departures returned"])]

    df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        # Delay is the whole point: it is the gap between plan and reality.
        (
            pl.col("expected_departure").str.to_datetime(strict=False)
            - pl.col("aimed_departure").str.to_datetime(strict=False)
        )
        .dt.total_seconds()
        .alias("delay_s")
    )
    path = write_partition(df, f"{name}_departures", stamp)
    notes.append(f"{df['monitoring_ref'].n_unique()} stops, median delay "
                 f"{df['delay_s'].median()}s")
    return [Written(f"{name}_departures", stamp, df.height, path, notes)]


def poll_network(key: str, cli) -> list[Written]:
    """One SIRI EstimatedTimetable snapshot of the entire network.

    Preferred over per-stop polling: one request instead of thousands, and it
    covers every line rather than the venues chosen in advance. Per-stop polling
    remains available for the cases where a specific quay matters.
    """
    stamp = _stamp()
    resp = cli.get(ESTIMATED_TIMETABLE_URL, headers={"apiKey": key})
    resp.raise_for_status()
    payload = resp.content
    _archive(payload, f"estimated-timetable-{stamp}.json", ESTIMATED_TIMETABLE_URL, "network snapshot", None)

    body = json.loads(payload)
    deliveries = (
        body.get("Siri", {})
        .get("ServiceDelivery", {})
        .get("EstimatedTimetableDelivery", [])
    )
    rows = []
    for delivery in deliveries:
        for frame in delivery.get("EstimatedJourneyVersionFrame", []):
            for journey in frame.get("EstimatedVehicleJourney", []):
                line = (journey.get("LineRef") or {}).get("value")
                for call in (journey.get("EstimatedCalls") or {}).get("EstimatedCall", []):
                    rows.append(
                        {
                            "polled_at": stamp,
                            "line_ref": line,
                            "stop_ref": (call.get("StopPointRef") or {}).get("value"),
                            "vehicle_journey": (
                                journey.get("FramedVehicleJourneyRef") or {}
                            ).get("DatedVehicleJourneyRef"),
                            "aimed_departure": call.get("AimedDepartureTime"),
                            "expected_departure": call.get("ExpectedDepartureTime"),
                            "aimed_arrival": call.get("AimedArrivalTime"),
                            "expected_arrival": call.get("ExpectedArrivalTime"),
                            "departure_status": call.get("DepartureStatus"),
                        }
                    )

    if not rows:
        return [Written(f"{name}_network", stamp, 0, None, ["no journeys returned"])]

    df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        (
            pl.col("expected_departure").str.to_datetime(strict=False)
            - pl.col("aimed_departure").str.to_datetime(strict=False)
        )
        .dt.total_seconds()
        .alias("delay_s")
    )
    path = write_partition(df, f"{name}_network", stamp)
    return [
        Written(
            f"{name}_network",
            stamp,
            df.height,
            path,
            [f"{df['line_ref'].n_unique()} lignes, {df['stop_ref'].n_unique()} arrets"],
        )
    ]


def watched_stops() -> list[str]:
    """Stops to monitor, from the venue registry's serving stations.

    Only used for per-stop polling. These come from the published real-time
    perimeter rather than being constructed, so every one is known to be covered.
    """
    from ..venues import monitoring_refs

    return monitoring_refs()


def fetch(
    stops: list[str] | None = None,
    disruptions_only: bool = False,
    per_stop: bool = False,
) -> list[Written]:
    """Poll PRIM. Defaults to disruptions plus one network-wide snapshot."""
    key = api_key()
    with client() as cli:
        out = poll_disruptions(key, cli)
        if disruptions_only:
            return out
        if per_stop:
            targets = stops if stops is not None else watched_stops()
            if targets:
                out.extend(poll_departures(key, targets, cli))
            else:
                out.append(
                    Written(
                        f"{name}_departures",
                        _stamp(),
                        0,
                        None,
                        ["no watched stops -- import the real-time perimeter first"],
                    )
                )
        else:
            out.extend(poll_network(key, cli))
    return out


def probe() -> str:
    """Verify the endpoint contracts against a live key. Writes nothing."""
    key = api_key()
    lines = []
    with client() as cli:
        resp = cli.get(DISRUPTIONS_URL, headers={"apiKey": key})
        lines.append(f"disruptions: HTTP {resp.status_code}, {len(resp.content):,} bytes")
        if resp.is_success:
            body = resp.json()
            lines.append(f"  top-level keys: {list(body)[:8]}")

        stops = watched_stops()[:1]
        if stops:
            r2 = cli.get(STOP_MONITORING_URL, params={"MonitoringRef": stops[0]}, headers={"apiKey": key})
            lines.append(f"stop-monitoring {stops[0]}: HTTP {r2.status_code}, {len(r2.content):,} bytes")
            if r2.is_success:
                lines.append(f"  top-level keys: {list(r2.json())[:8]}")
    return "\n".join(lines)
