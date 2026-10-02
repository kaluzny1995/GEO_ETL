"""Fetch and cache USGS HANS notice details for ``single_etl/usgs_volcano_etl.py``.

The CSV feed only carries the notice's headline fields (volcano, alert level,
notice identifier) — the coordinates, region and status line of each volcano
live behind the notice API referenced by ``notice_data_url``::

    https://volcanoes.usgs.gov/hans-public/api/notice/getNotice/<notice_identifier>

The response holds one ``notice_sections`` entry per volcano of the notice
(``vName``, ``vnum``, ``lat``, ``lng``, ``region``, ``synopsis``, …). This
module resolves every requested ``(notice_identifier, vnum)`` pair against a
cache file, querying the API only for pairs the cache does not cover yet::

    cache/usgs_volcano_notices.csv    notice_identifier, vnum, notice_type,
                                      latitude, longitude, region, synopsis

Notices are immutable once published, so a cached entry never goes stale and
the file grows by only the handful of new notices each day. The cache is read
on the driver: the Spark containers only mount ``GEO_Download/data``, so
nothing under ``cache/`` is reachable from the executors and the answers are
embedded as column literals (see ``_enrichment_columns`` in the ETL script).

A notice is fetched once even when several of its volcanoes are requested, and
requests that fail are *not* cached: they are retried on the next run, and when
nothing could be fetched the build fails instead of silently leaving holes in
the cache (same policy as ``utils/reverse_geocode.py``).
"""

from __future__ import annotations

import csv
import json
import logging
import os
import urllib.request
from collections.abc import Iterable

DEFAULT_API_URL = "https://volcanoes.usgs.gov/hans-public/api/notice/getNotice"
DEFAULT_OUTPUT = os.path.join("cache", "usgs_volcano_notices.csv")
OUTPUT_FIELDS = ["notice_identifier", "vnum", "notice_type", "latitude", "longitude", "region", "synopsis"]
USER_AGENT = "GEO_ETL/1.0"


def notice_key(notice_identifier: str, vnum: str | int) -> str:
    """Cache key of one volcano section of one notice.

    Args:
        notice_identifier: The notice's DOI identifier
            (``DOI-USGS-AVO-2026-09-29T18:26:19+00:00``).
        vnum: The Smithsonian/USGS volcano number of the section (``311120``).

    Returns:
        ``"<notice_identifier>|<vnum>"``.
    """
    return f"{notice_identifier}|{vnum}"


def parse_notice(data: dict, vnum: str | int) -> dict[str, str]:
    """Extract one volcano's fields from a fetched notice payload.

    Args:
        data: Decoded JSON body of the notice endpoint.
        vnum: Volcano number whose section is wanted — a notice carries one
            section per volcano (plus region-wide sections with no ``vnum``).

    Returns:
        ``notice_type``, ``latitude``, ``longitude``, ``region`` and
        ``synopsis`` as strings; a field the API left out (``None``) becomes
        an empty string, which the ETL turns into ``NULL``.

    Raises:
        RuntimeError: When no section of the notice carries *vnum* (the feed
            row and the notice payload disagree — fail loudly, never guess).
    """
    sections = data.get("notice_sections") or []
    section = next((s for s in sections if str(s.get("vnum") or "") == str(vnum)), None)
    if section is None:
        available = ", ".join(str(s.get("vnum")) for s in sections) or "none"
        raise RuntimeError(
            f"notice {data.get('notice_identifier', '?')} has no section for vnum {vnum} (has: {available})"
        )
    latitude = section.get("lat")
    longitude = section.get("lng")
    return {
        "notice_type": str(data.get("notice_type") or "").strip(),
        "latitude": str(latitude) if latitude is not None else "",
        "longitude": str(longitude) if longitude is not None else "",
        "region": str(section.get("region") or "").strip(),
        "synopsis": str(section.get("synopsis") or "").strip(),
    }


def fetch_notice(notice_identifier: str, api_url: str = DEFAULT_API_URL) -> dict:
    """Download the payload of a single notice.

    Args:
        notice_identifier: The notice's DOI identifier.
        api_url: Notice endpoint base URL.

    Returns:
        The decoded JSON body.

    Raises:
        OSError: When the request fails (no network, HTTP error, bad body).
    """
    request = urllib.request.Request(f"{api_url}/{notice_identifier}", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def load_notices(path: str = DEFAULT_OUTPUT) -> dict[str, dict[str, str]]:
    """Read the cache CSV into ``notice_key -> {field: value}``.

    Args:
        path: Cache file to read; a missing file yields an empty cache.

    Returns:
        One entry per stored ``(notice, vnum)`` pair; malformed rows keep
        empty answers.
    """
    if not os.path.exists(path):
        return {}
    with open(path, newline="") as handle:
        rows = csv.DictReader(handle)
        return {
            notice_key(row["notice_identifier"], row["vnum"]): {
                field: (row.get(field) or "").strip() for field in OUTPUT_FIELDS[2:]
            }
            for row in rows
        }


def save_notices(mapping: dict[str, dict[str, str]], path: str = DEFAULT_OUTPUT) -> int:
    """Write ``notice_key -> {field: value}`` back to the cache.

    Args:
        mapping: Full cache contents (existing plus newly fetched entries).
        path: Cache CSV to write; the parent directory is created when missing.

    Returns:
        The number of notice sections written.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(OUTPUT_FIELDS)
        for key in sorted(mapping):
            notice_identifier, vnum = key.rsplit("|", 1)
            values = mapping[key]
            writer.writerow(
                [
                    notice_identifier,
                    vnum,
                    values["notice_type"],
                    values["latitude"],
                    values["longitude"],
                    values["region"],
                    values["synopsis"],
                ]
            )
    return len(mapping)


def notice_map(
    keys: Iterable[tuple[str, str | int]],
    *,
    api_url: str = DEFAULT_API_URL,
    output: str = DEFAULT_OUTPUT,
    logger: logging.Logger | None = None,
) -> dict[str, dict[str, str]]:
    """Resolve every ``(notice_identifier, vnum)`` pair, fetching only cache misses.

    Each notice is downloaded at most once per call, however many of its
    volcanoes are requested.

    Args:
        keys: ``(notice_identifier, vnum)`` pairs as collected from the DataFrame.
        api_url: Notice endpoint base URL.
        output: Cache CSV to read and extend.
        logger: Logger for fetch messages; defaults to the module logger.

    Returns:
        ``notice_key -> {notice_type, latitude, longitude, region, synopsis}``
        covering every requested pair; entries fetched for this call are
        already persisted to *output*.

    Raises:
        RuntimeError: When one or more new pairs could not be fetched or a
            notice payload has no section for the requested volcano.
            Successful answers are still cached, failed ones retry next run.
    """
    log = logger or logging.getLogger(__name__)
    mapping = load_notices(output)
    requested = {notice_key(identifier, vnum) for identifier, vnum in keys}
    missing = sorted(requested - mapping.keys())
    if not missing:
        return {key: mapping[key] for key in requested if key in mapping}

    log.info(f"Volcano notices: {len(missing)} pair(s) missing from {output} — querying the API...")
    payloads: dict[str, dict] = {}
    failures: list[str] = []
    fetched = 0
    for key in missing:
        notice_identifier, vnum = key.rsplit("|", 1)
        try:
            if notice_identifier not in payloads:
                payloads[notice_identifier] = fetch_notice(notice_identifier, api_url)
            mapping[key] = parse_notice(payloads[notice_identifier], vnum)
        except Exception as exc:
            failures.append(f"{key} ({exc})")
        else:
            fetched += 1
    if fetched:
        save_notices(mapping, output)
        log.info(f"Volcano notice cache updated: {fetched} fetched, {len(mapping)} total → {output}")
    if failures:
        raise RuntimeError(
            f"volcano notice fetch failed for {len(failures)} of {len(missing)} new pair(s): "
            f"{'; '.join(failures[:3])}" + ("…" if len(failures) > 3 else "")
        )
    return {key: mapping[key] for key in requested if key in mapping}
