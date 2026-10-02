"""Build the NIFC jurisdiction unit-code → full-name lookup used by ``single_etl/nifc_fire_etl.py``.

The WFIGS perimeter layer records only the jurisdiction's short code
(``attr_POOJurisdictionalUnit``, e.g. ``WAOWF``, ``CAYNP``) while
``attr_POOJurisdictionalAgency`` holds the even shorter agency code
(``USFS``, ``NPS``, …) with no published domain — so neither can be rendered
directly as a ``place_location``. The authoritative names live in the public
ArcGIS layer ``DMP_JurisdictionalUnits_Public``:

    JurisdictionalUnitID_sansUS → JurisdictionalUnitName
        WAOWF → Okanogan-Wenatchee National Forest
        CAYNP → Yosemite National Park

One distinct-values query returns the whole universe (≈1750 code/name pairs)
without pagination; a handful of codes carry two names and the alphabetically
first one wins so the output is deterministic. Write::

    cache/nifc_unit_names.csv    unit_code,unit_name

Run::

    python -m utils.fetch_nifc_units

``single_etl/nifc_fire_etl.py`` calls :func:`unit_name_map` with the codes seen
in the data: the file is fetched when missing, and codes absent from an existing
file trigger a refresh. Codes that resolve nowhere (the layer does not keep
retired codes such as the pre-2025 ``NVNV0100`` format) are simply left out and
the ETL falls back to the raw code.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import urllib.parse
import urllib.request

DEFAULT_API_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "DMP_JurisdictionalUnits_Public/FeatureServer/0/query"
)
DEFAULT_OUTPUT = os.path.join("cache", "nifc_unit_names.csv")
OUTPUT_FIELDS = ["unit_code", "unit_name"]
WHERE_CLAUSE = "JurisdictionalUnitID_sansUS IS NOT NULL AND JurisdictionalUnitID_sansUS <> ''"
CODE_FIELD = "JurisdictionalUnitID_sansUS"
NAME_FIELD = "JurisdictionalUnitName"


def _fetch_pairs(api_url: str) -> set[tuple[str, str]]:
    """Download every distinct ``(code, name)`` pair of the jurisdiction layer.

    Args:
        api_url: ArcGIS ``FeatureServer/0/query`` endpoint.

    Returns:
        The distinct non-empty code/name pairs.

    Raises:
        RuntimeError: When the endpoint errors out or returns no usable rows.
    """
    params = {
        "where": WHERE_CLAUSE,
        "outFields": f"{CODE_FIELD},{NAME_FIELD}",
        "returnDistinctValues": "true",
        "returnGeometry": "false",
        "f": "json",
    }
    url = f"{api_url}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=120) as response:
        data = json.load(response)

    if isinstance(data, dict) and "error" in data:
        error = data["error"]
        message = error.get("message") if isinstance(error, dict) else str(error)
        raise RuntimeError(f"NIFC jurisdiction layer rejected the query: {message}")

    features = data.get("features") if isinstance(data, dict) else None
    pairs = {
        (str(attrs[CODE_FIELD]).strip(), str(attrs[NAME_FIELD]).strip())
        for attrs in (feature.get("attributes") or {} for feature in features or [])
        if attrs.get(CODE_FIELD) and attrs.get(NAME_FIELD)
    }
    if not pairs:
        raise RuntimeError("NIFC jurisdiction layer returned no code/name pairs — nothing written")
    return pairs


def _name_by_code(pairs: set[tuple[str, str]]) -> dict[str, str]:
    """Collapse code/name pairs into a deterministic ``code → name`` map.

    Args:
        pairs: Distinct code/name pairs from the layer.

    Returns:
        One name per code, the alphabetically first when a code has several.
    """
    names: dict[str, str] = {}
    for code, name in sorted(pairs):
        names.setdefault(code, name)
    return names


def load_unit_names(output: str = DEFAULT_OUTPUT) -> dict[str, str]:
    """Read the lookup CSV into a ``code → name`` map.

    Args:
        output: Lookup CSV to read.

    Returns:
        The cached names, or an empty map when the file does not exist yet.
    """
    if not os.path.exists(output):
        return {}
    with open(output, newline="") as handle:
        return {
            row["unit_code"].strip(): row["unit_name"].strip()
            for row in csv.DictReader(handle)
            if row.get("unit_code") and row.get("unit_name")
        }


def _write(names: dict[str, str], output: str) -> None:
    """Write ``names`` to the lookup CSV, creating the directory when needed.

    Args:
        names: ``code → name`` map to persist.
        output: Lookup CSV to write.
    """
    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    with open(output, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows({"unit_code": code, "unit_name": names[code]} for code in sorted(names))


def build_lookup(api_url: str = DEFAULT_API_URL, output: str = DEFAULT_OUTPUT) -> int:
    """Refresh the whole lookup CSV from the jurisdiction layer.

    Args:
        api_url: ArcGIS query endpoint.
        output: Lookup CSV to write.

    Returns:
        The number of unit codes written.
    """
    names = _name_by_code(_fetch_pairs(api_url))
    _write(names, output)
    return len(names)


def unit_name_map(codes: set[str], output: str = DEFAULT_OUTPUT, api_url: str = DEFAULT_API_URL) -> dict[str, str]:
    """Names for *codes*, fetching the layer when the cache is missing or stale.

    Args:
        codes: Unit codes seen in the data; empty/blank codes are ignored.
        output: Lookup CSV to read (and refresh when a code is unknown).
        api_url: ArcGIS query endpoint used for the refresh.

    Returns:
        ``code → name`` for every code that resolves; unresolved codes are
        absent and the caller falls back to the raw code.

    Raises:
        RuntimeError: When a refresh is needed and the layer fetch fails.
    """
    wanted = {code.strip() for code in codes if code and code.strip()}
    names = load_unit_names(output)
    if wanted - names.keys():
        build_lookup(api_url, output)
        names = load_unit_names(output)
    return {code: names[code] for code in sorted(wanted & names.keys())}


def main() -> None:
    """Refresh the unit-code lookup CSV."""
    parser = argparse.ArgumentParser(description="Build the NIFC jurisdiction unit-name lookup")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="ArcGIS jurisdiction layer query endpoint")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="lookup CSV to write")
    args = parser.parse_args()

    try:
        written = build_lookup(args.api_url, args.output)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"{written} jurisdiction unit names -> {args.output}")


if __name__ == "__main__":
    main()
