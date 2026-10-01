"""Reverse geocode coordinates to an ISO country/state pair with a cached lookup.

Used by the ``single_etl/`` scripts to resolve ``country_code`` /
``state_province`` when the source text carries no location. The endpoint
(Nominatim / OpenStreetMap, keyless) is queried only for coordinates missing
from the cache file, which is written back after every run::

    cache/reverse_geocode.csv    latitude, longitude, country_code, state_province

Coordinates are keyed at 4 decimal places (~11 m), so a point is queried once
and reused by every later run. The CSV is read on the driver: the Spark
containers only mount ``GEO_Download/data``, so nothing under ``cache/`` is
reachable from the executors and the answers are embedded as column literals
(see ``_geocode_columns`` in the ETL scripts).

Nominatim's usage policy allows at most one request per second from an
identifiable client, so every request waits :data:`REQUEST_DELAY_SECONDS`
first. A coordinate the API answers with no country (open ocean) is cached as
an empty ``country_code`` — that is a real answer, and the ETL turns it into
its fallback ``ZZ``. Requests that fail are *not* cached: they are retried on
the next run, and when nothing could be fetched the build fails instead of
silently leaving holes in the cache.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from collections.abc import Iterable

DEFAULT_API_URL = "https://nominatim.openstreetmap.org/reverse"
DEFAULT_OUTPUT = os.path.join("cache", "reverse_geocode.csv")
OUTPUT_FIELDS = ["latitude", "longitude", "country_code", "state_province"]
KEY_DECIMALS = 4
USER_AGENT = "GEO_ETL/1.0"
REQUEST_DELAY_SECONDS = 1.1


def coord_key(latitude: float | str, longitude: float | str) -> str:
    """Cache key of a coordinate: both values rounded to 4 decimals (~11 m).

    Args:
        latitude: Latitude in decimal degrees.
        longitude: Longitude in decimal degrees.

    Returns:
        ``"<latitude>,<longitude>"`` with both parts fixed to
        :data:`KEY_DECIMALS` decimals.
    """
    return f"{float(latitude):.{KEY_DECIMALS}f},{float(longitude):.{KEY_DECIMALS}f}"


def parse_response(data: dict) -> tuple[str, str]:
    """Turn a Nominatim reverse geocode response into a country/state pair.

    Args:
        data: JSON body of the endpoint (``address.country_code``,
            ``address.ISO3166-2-lvl4``, …).

    Returns:
        ``(country_code, state_province)`` — the ISO 3166-1 alpha-2 country
        and the subdivision part of the most specific ISO 3166-2 code the
        address carries (``US-TX`` → ``TX``); the state level (``lvl4``)
        wins over the district levels below it. Both are empty strings when
        the API found neither: open water has no address at all, and many
        countries' waters have no subdivision.
    """
    address = data.get("address") or {}
    country = str(address.get("country_code") or "").strip().upper()
    state = ""
    for level in ("lvl4", "lvl5", "lvl6"):
        subdivision = str(address.get(f"ISO3166-2-{level}") or "").strip()
        if "-" in subdivision:
            state = subdivision.rsplit("-", 1)[-1]
            break
    return country, state


def reverse_geocode(latitude: float, longitude: float, api_url: str = DEFAULT_API_URL) -> tuple[str, str]:
    """Query the endpoint for one coordinate.

    Args:
        latitude: Latitude in decimal degrees.
        longitude: Longitude in decimal degrees.
        api_url: Reverse geocode endpoint.

    Returns:
        ``(country_code, state_province)`` from :func:`parse_response`.

    Raises:
        OSError: When the request fails (no network, HTTP error, bad body).
    """
    time.sleep(REQUEST_DELAY_SECONDS)
    query = urllib.parse.urlencode(
        {
            "lat": float(latitude),
            "lon": float(longitude),
            "format": "jsonv2",
            "addressdetails": 1,
            "zoom": 14,
            "accept-language": "en",
        }
    )
    request = urllib.request.Request(f"{api_url}?{query}", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.load(response)
    return parse_response(data)


def load_cache(path: str = DEFAULT_OUTPUT) -> dict[str, tuple[str, str]]:
    """Read the cache CSV into ``coord_key -> (country_code, state_province)``.

    Args:
        path: Cache file to read; a missing file yields an empty cache.

    Returns:
        One entry per stored coordinate; malformed rows keep empty answers.
    """
    if not os.path.exists(path):
        return {}
    with open(path, newline="") as handle:
        return {
            coord_key(row["latitude"], row["longitude"]): (
                (row["country_code"] or "").strip(),
                (row["state_province"] or "").strip(),
            )
            for row in csv.DictReader(handle)
        }


def save_cache(mapping: dict[str, tuple[str, str]], path: str = DEFAULT_OUTPUT) -> int:
    """Write ``coord_key -> (country_code, state_province)`` back to the cache.

    Args:
        mapping: Full cache contents (existing plus newly fetched entries).
        path: Cache CSV to write; the parent directory is created when missing.

    Returns:
        The number of coordinates written.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(OUTPUT_FIELDS)
        for key in sorted(mapping):
            latitude, longitude = key.split(",")
            country, state = mapping[key]
            writer.writerow([latitude, longitude, country, state])
    return len(mapping)


def geocode_map(
    points: Iterable[tuple[float, float]],
    *,
    api_url: str = DEFAULT_API_URL,
    output: str = DEFAULT_OUTPUT,
    logger: logging.Logger | None = None,
) -> dict[str, tuple[str, str]]:
    """Resolve every coordinate in *points*, fetching only what the cache misses.

    Args:
        points: ``(latitude, longitude)`` pairs as collected from the DataFrame.
        api_url: Reverse geocode endpoint.
        output: Cache CSV to read and extend.
        logger: Logger for fetch messages; defaults to the module logger.

    Returns:
        ``coord_key -> (country_code, state_province)`` covering every point;
        answers fetched for this call are already persisted to *output*.

    Raises:
        RuntimeError: When one or more new coordinates could not be fetched.
            Successful answers are still cached, failed ones retry next run.
    """
    log = logger or logging.getLogger(__name__)
    mapping = load_cache(output)
    missing = sorted({coord_key(*point) for point in points} - mapping.keys())
    if not missing:
        return mapping

    log.info(f"Reverse geocode: {len(missing)} coordinate(s) missing from {output} — querying the API...")
    failures: list[str] = []
    fetched = 0
    for key in missing:
        latitude, longitude = (float(part) for part in key.split(","))
        try:
            mapping[key] = reverse_geocode(latitude, longitude, api_url)
        except Exception as exc:
            failures.append(f"{key} ({exc})")
        else:
            fetched += 1
    if fetched:
        save_cache(mapping, output)
        log.info(f"Reverse geocode cache updated: {fetched} fetched, {len(mapping)} total → {output}")
    if failures:
        raise RuntimeError(
            f"reverse geocode failed for {len(failures)} of {len(missing)} new coordinate(s): "
            f"{'; '.join(failures[:3])}" + ("…" if len(failures) > 3 else "")
        )
    return mapping
