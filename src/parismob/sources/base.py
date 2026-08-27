"""The contract every collector honours, and the HTTP helper that enforces it.

A collector's job is narrow: get bytes, archive them untouched, record the fetch,
parse, write one whole partition. `download()` below does the first three so no
collector can forget them, and so the manifest is written by exactly one code path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

import httpx

from ..config import TIMEOUT, USER_AGENT
from ..lake import already_have, raw_path, record_fetch, sha256_bytes


@dataclass
class Fetched:
    """One archived payload."""

    path: Path
    data: bytes
    digest: str
    url: str
    from_cache: bool = False
    """True when these bytes came off local disk instead of the network."""


@dataclass
class Written:
    """The result of one collector run, for the CLI to report honestly."""

    source: str
    partition: str
    rows: int
    path: Path | None = None
    notes: list[str] = field(default_factory=list)


@runtime_checkable
class Source(Protocol):
    """What `parismob fetch <name>` needs from a collector module."""

    name: str

    def fetch(self, **kwargs) -> list[Written]:
        """Collect, parse and write. Must be safe to re-run."""
        ...


def client() -> httpx.Client:
    """One configured client. `follow_redirects` matters: the IDFM file endpoints
    redirect to object storage, and gzip is negotiated and decoded transparently.
    """
    return httpx.Client(
        headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"},
        timeout=TIMEOUT,
        follow_redirects=True,
    )


def download(
    url: str,
    source: str,
    filename: str,
    *,
    force: bool = False,
    note: str = "",
    cli: httpx.Client | None = None,
) -> Fetched:
    """Fetch, archive the raw bytes, and record the fetch in the manifest.

    Archiving happens *before* any parsing, because the parser is the part most
    likely to be wrong later. Re-parsing is cheap; re-downloading may be impossible
    once a feed has rolled over.

    If the file is already on disk and its content hash is already in the manifest,
    the network is skipped entirely -- so `fetch --all` is cheap to re-run. `force`
    overrides that, which is how you pick up an upstream correction.
    """
    dest = raw_path(source, filename)

    if dest.exists() and not force:
        data = dest.read_bytes()
        digest = sha256_bytes(data)
        if already_have(digest):
            return Fetched(path=dest, data=data, digest=digest, url=url, from_cache=True)

    owned = cli is None
    c = cli or client()
    try:
        resp = c.get(url)
        resp.raise_for_status()
        data = resp.content
    finally:
        if owned:
            c.close()

    digest = sha256_bytes(data)
    dest.write_bytes(data)
    record_fetch(
        source=source,
        url=url,
        path=dest,
        n_bytes=len(data),
        digest=digest,
        note=note,
    )
    return Fetched(path=dest, data=data, digest=digest, url=url, from_cache=False)


def ods_export(
    dataset: str,
    *,
    base: str,
    fmt: str = "csv",
    cli: httpx.Client | None = None,
) -> bytes:
    """Download a whole ODS dataset in one request, bypassing the offset ceiling.

    `/records` refuses `offset` beyond 10,000, which rules it out for the stop
    referentials (37,941 and 18,008 rows). `/exports` has no such limit and is a
    single request rather than hundreds. Returns raw bytes so the caller can
    archive them before parsing, per the usual invariant.
    """
    owned = cli is None
    c = cli or client()
    try:
        resp = c.get(f"{base}/api/explore/v2.1/catalog/datasets/{dataset}/exports/{fmt}")
        resp.raise_for_status()
        return resp.content
    finally:
        if owned:
            c.close()


def ods_records(
    dataset: str,
    *,
    base: str,
    select: str | None = None,
    where: str | None = None,
    page: int = 100,
    cli: httpx.Client | None = None,
) -> list[dict]:
    """Page through an Opendatasoft Explore v2.1 dataset until it is exhausted.

    ODS caps `limit` at 100 and refuses `offset` beyond 10,000, so anything larger
    has to be split by a `where` clause rather than paged. Callers that might cross
    that ceiling are responsible for slicing; this raises rather than silently
    truncating, because a collector that quietly returns 10,000 of 40,000 rows is
    exactly the kind of failure this project is built to avoid.
    """
    owned = cli is None
    c = cli or client()
    out: list[dict] = []
    try:
        offset = 0
        while True:
            params: dict[str, str | int] = {"limit": page, "offset": offset}
            if select:
                params["select"] = select
            if where:
                params["where"] = where
            resp = c.get(f"{base}/api/explore/v2.1/catalog/datasets/{dataset}/records", params=params)
            resp.raise_for_status()
            body = resp.json()
            results = body.get("results", [])
            out.extend(results)
            total = body.get("total_count", 0)
            offset += page
            if offset >= total or not results:
                break
            if offset >= 10_000 and offset < total:
                raise RuntimeError(
                    f"{dataset}: {total} records exceeds the ODS 10,000-offset ceiling. "
                    f"Slice the fetch with a `where` clause instead of paging past it."
                )
    finally:
        if owned:
            c.close()
    return out
