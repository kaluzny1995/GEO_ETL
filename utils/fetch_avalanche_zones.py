"""Build the static avalanche zone centroid lookup used by ``single_etl/avalanche_org_etl.py``.

Downloads the avalanche.org map-layer GeoJSON, computes the area weighted
centroid of every forecast zone polygon and writes::

    cache/avalanche_zone_centroids.csv    place_location, state, latitude, longitude

The ETL joins that file by ``place_location`` + ``state``, because zone ids are
recycled over time (97 ids for 85 zones in the historical data) while zone names
stay stable.

Run::

    python -m utils.fetch_avalanche_zones --day 2026-09-30 --day 2026-09-01

``--day`` is repeatable and the days are merged: zones come and go (the
``Whitefish Range`` zone left the map layer after 2026-09-25), so a window of
days covers zones that are absent from today's feed. Without ``--day`` the
window from :func:`lookup_days` (today, 10 and 30 days back) is fetched.
``single_etl/avalanche_org_etl.py`` calls :func:`build_lookup` with the same
window when the file is missing, so the ETL can also build the lookup itself.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import urllib.request
from itertools import pairwise

DEFAULT_API_URL = "https://api.avalanche.org/v2/public/products/map-layer"
DEFAULT_OUTPUT = os.path.join("cache", "avalanche_zone_centroids.csv")
OUTPUT_FIELDS = ["place_location", "state", "latitude", "longitude"]


def _ring_centroid(ring: list[list[float]]) -> tuple[float, float, float]:
    """Centroid and area of one closed GeoJSON ring of ``[lon, lat]`` vertices.

    Args:
        ring: Ring coordinates, first vertex repeated at the end.

    Returns:
        ``(lon, lat, area)``; a degenerate ring falls back to its vertex average
        with an area of ``0``.
    """
    area = 0.0
    lon = 0.0
    lat = 0.0
    for (x0, y0), (x1, y1) in pairwise(ring):
        cross = x0 * y1 - x1 * y0
        area += cross
        lon += (x0 + x1) * cross
        lat += (y0 + y1) * cross
    area *= 0.5
    if abs(area) < 1e-12 or len(ring) < 2:
        points = ring[:-1] if len(ring) > 1 and ring[0] == ring[-1] else ring
        lons = [point[0] for point in points] or [0.0]
        lats = [point[1] for point in points] or [0.0]
        return sum(lons) / len(lons), sum(lats) / len(lats), 0.0
    return lon / (6 * area), lat / (6 * area), abs(area)


def _polygon_centroid(rings: list[list[list[float]]]) -> tuple[float, float, float]:
    """Area weighted centroid of a GeoJSON polygon: exterior ring minus its holes.

    Args:
        rings: ``[exterior, hole, hole, ...]`` rings of one polygon.

    Returns:
        ``(lon, lat, area)``; falls back to the exterior ring when the holes
        cancel out the whole area.
    """
    ext_lon, ext_lat, ext_area = _ring_centroid(rings[0])
    lon, lat, total = ext_lon * ext_area, ext_lat * ext_area, ext_area
    for hole in rings[1:]:
        hole_lon, hole_lat, hole_area = _ring_centroid(hole)
        lon -= hole_lon * hole_area
        lat -= hole_lat * hole_area
        total -= hole_area
    if total <= 0:
        return ext_lon, ext_lat, ext_area
    return lon / total, lat / total, total


def _geometry_centroid(geometry: dict | None) -> tuple[float, float] | None:
    """Centroid of a GeoJSON ``Polygon`` or ``MultiPolygon``.

    Args:
        geometry: GeoJSON geometry object, or ``None``.

    Returns:
        ``(lon, lat)``, or ``None`` when the geometry is missing or empty.
    """
    if not geometry:
        return None
    gtype = geometry.get("type")
    coordinates = geometry.get("coordinates") or []
    if gtype == "Polygon":
        polygons = [coordinates]
    elif gtype == "MultiPolygon":
        polygons = coordinates
    else:
        return None
    lon = 0.0
    lat = 0.0
    total = 0.0
    for polygon in polygons:
        if not polygon:
            continue
        poly_lon, poly_lat, area = _polygon_centroid(polygon)
        lon += poly_lon * area
        lat += poly_lat * area
        total += area
    if total <= 0:
        return None
    return lon / total, lat / total


def _fetch(api_url: str, days: list[str]) -> list[dict]:
    """Download the map-layer ``FeatureCollection`` of every requested day.

    Args:
        api_url: Avalanche.org map-layer endpoint.
        days: Forecast days in ``YYYY-MM-DD``, fetched in the given order.

    Returns:
        The GeoJSON features of all requested days.
    """
    features: list[dict] = []
    for day in days:
        with urllib.request.urlopen(f"{api_url}?day={day}", timeout=60) as response:
            data = json.load(response)
        day_features = data.get("features") if isinstance(data, dict) else None
        if isinstance(day_features, list):
            features.extend(day_features)
    return features


def _lookup_rows(features: list[dict]) -> list[dict]:
    """Turn GeoJSON features into lookup rows keyed by ``place_location`` + ``state``.

    Args:
        features: Map-layer features from one or more days; the first centroid
            seen for a zone wins, because a zone's polygon does not change.

    Returns:
        One row per distinct zone with its polygon centroid, sorted by state and
        zone name. Features without a name or a usable geometry are skipped.
    """
    rows: dict[tuple[str, str], dict] = {}
    for feature in features:
        props = feature.get("properties") or {}
        name = str(props.get("name") or "").strip()
        state = str(props.get("state") or "").strip()
        centroid = _geometry_centroid(feature.get("geometry"))
        if not name or centroid is None:
            continue
        rows.setdefault(
            (name, state),
            {
                "place_location": name,
                "state": state,
                "latitude": round(centroid[1], 6),
                "longitude": round(centroid[0], 6),
            },
        )
    return sorted(rows.values(), key=lambda row: (row["state"], row["place_location"]))


def lookup_days(today: dt.date | None = None) -> list[str]:
    """Forecast days that together cover every zone of the lookup.

    Zones come and go, so a spaced window keeps zones that are absent from
    today's feed covered: today, 10 days back and 30 days back.

    Args:
        today: Reference day, or ``None`` for the current local date.

    Returns:
        Three forecast days in ``YYYY-MM-DD``, newest first.
    """
    reference = today or dt.date.today()
    return [(reference - dt.timedelta(days=offset)).isoformat() for offset in (0, 10, 30)]


def build_lookup(days: list[str], api_url: str = DEFAULT_API_URL, output: str = DEFAULT_OUTPUT) -> int:
    """Download the map-layer features of *days* and write the centroid lookup CSV.

    The output directory is created when it does not exist yet.

    Args:
        days: Forecast days in ``YYYY-MM-DD``, fetched in the given order.
        api_url: Avalanche.org map-layer endpoint.
        output: Lookup CSV to write.

    Returns:
        The number of zone rows written.

    Raises:
        RuntimeError: When the days yield no usable zone geometry; nothing is
            written in that case.
    """
    rows = _lookup_rows(_fetch(api_url, days))
    if not rows:
        raise RuntimeError(f"No zone geometries returned for {', '.join(days)} — nothing written")

    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    with open(output, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main() -> None:
    """Download zone polygons and write the centroid lookup CSV."""
    parser = argparse.ArgumentParser(description="Build the avalanche zone centroid lookup")
    parser.add_argument(
        "--day",
        action="append",
        dest="days",
        help="map-layer day (YYYY-MM-DD); repeatable, defaults to today, 10 and 30 days back",
    )
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="avalanche.org map-layer endpoint")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="lookup CSV to write")
    args = parser.parse_args()
    days = args.days or lookup_days()

    try:
        written = build_lookup(days, args.api_url, args.output)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"{written} zone centroids over {len(days)} day(s) -> {args.output}")


if __name__ == "__main__":
    main()
