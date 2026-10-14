"""Canonical-unit harmonisation for cross-source metrics.

Some ``metric_name`` values are reported by several sources in different
physical units (``wind speed`` in knots vs km/h, ``area`` in acres/ha/km²,
``discharge`` in ft³/s vs m³/s). This module maps each such metric onto a
single canonical unit for the whole world and converts the value accordingly.

The earthquake ``magnitude`` is a special case. The moment-magnitude reports
— the USGS ``mw`` / ``mww`` / ``mwr`` / ``mwc`` / ``mwb`` family and GDACS's
``M`` — already are moment magnitudes, so only their label is restated as the
standard ``Mw``. The local, body-wave, duration and Lg scales (``ml``, ``md``,
``mb``, ``mb_lg``) are *converted* to the equivalent moment magnitude ``Mw*``
with the empirical rules USGS OF 1997/464 (page 4) publishes for each of them
and only then relabelled ``Mw``. Two labels carry no published equation and so
keep their own scale label untouched: ``mh`` (the USGS Magnitude Types table
gives its Equation as ``N/A`` — a non-standard, temporary designation) and
``mun`` (absent from that table altogether; in AQMS it means "unknown
magnitude").

The dimensionless ``vei``, the avalanche ``size`` class and the single-unit
``surface`` are left untouched.

Every unit that carries a power exponent is written with the superscript
index: ``km²``, ``m³/s``, ``N/m²``.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

#: Canonical unit per harmonised ``metric_name``.
CANONICAL_UNITS: dict[str, str] = {
    "wind speed": "km/h",
    "area": "km²",
    "discharge": "m³/s",
}

#: one knot is one nautical mile per hour = 1.852 km/h exactly.
KNOT_TO_KMH = 1.852
#: one international acre = 0.0040468564224 km² exactly.
ACRE_TO_SQ_KM = 0.0040468564224
#: one hectare = 0.01 km² exactly.
HECTARE_TO_SQ_KM = 0.01
#: one cubic foot per second = 0.028316846592 m³/s exactly.
CUBIC_FOOT_TO_CUBIC_METRE = 0.028316846592

#: Threshold below which a reported magnitude is scaled up to the equivalent
#: moment magnitude, and left alone above it. USGS OF 1997/464 (page 4)
#: publishes one empirical rule per scale — every one of them shaped
#: ``Mw* = 0.67 * (x + 1.5)`` when ``x`` is below the threshold, ``x``
#: otherwise:
#:
#: * ``mb``    — ``Mw*=0.67*(mb+1.5) if mb<3.0 else mb``
#: * ``ML``    — ``Mw*=0.67*(ML+1.5) if ML<4.0 else ML``
#: * ``MD``    — same as ``ML`` ("don't downweight"), so the same 4.0 threshold
#: * ``Mn``    — ``Mw*=0.67*(Mn+1.5) if Mn<3.0 else Mn``; the USGS reports the
#:   Nuttli ``Mn`` scale as ``mb_lg`` (``mb_Lg == Mn == m_N``)
_MW_RULES: dict[tuple[str, str], float] = {
    ("magnitude", "mb"): 3.0,
    ("magnitude", "ml"): 4.0,
    ("magnitude", "md"): 4.0,
    ("magnitude", "mb_lg"): 3.0,
}
#: slope and offset shared by every entry of ``_MW_RULES``.
_MW_SLOPE = 0.67
_MW_OFFSET = 1.5

#: Relabel a source unit to a new label without scaling the value: the
#: moment-magnitude family (and GDACS's ``M``) restated as ``Mw``, plus the
#: four scales ``_MW_RULES`` has just converted to ``Mw*``.
_RELABEL: dict[tuple[str, str], str] = {
    ("magnitude", "mw"): "Mw",
    ("magnitude", "mww"): "Mw",
    ("magnitude", "mwr"): "Mw",
    ("magnitude", "mwc"): "Mw",
    ("magnitude", "mwb"): "Mw",
    ("magnitude", "m"): "Mw",  # GDACS reports its earthquake magnitude as "M"
    ("magnitude", "mb"): "Mw",
    ("magnitude", "ml"): "Mw",
    ("magnitude", "md"): "Mw",
    ("magnitude", "mb_lg"): "Mw",
}

#: factor that converts a source unit into the canonical unit of its metric.
_FACTORS: dict[tuple[str, str], float] = {
    ("wind speed", "kts"): KNOT_TO_KMH,
    ("wind speed", "kt"): KNOT_TO_KMH,
    ("wind speed", "knots"): KNOT_TO_KMH,
    ("wind speed", "km/h"): 1.0,
    ("wind speed", "kmh"): 1.0,
    ("area", "acres"): ACRE_TO_SQ_KM,
    ("area", "acre"): ACRE_TO_SQ_KM,
    ("area", "ha"): HECTARE_TO_SQ_KM,
    ("area", "hectare"): HECTARE_TO_SQ_KM,
    ("area", "hectares"): HECTARE_TO_SQ_KM,
    ("area", "km2"): 1.0,
    ("area", "km²"): 1.0,
    ("discharge", "ft3/s"): CUBIC_FOOT_TO_CUBIC_METRE,
    ("discharge", "ft³/s"): CUBIC_FOOT_TO_CUBIC_METRE,
    ("discharge", "m3/s"): 1.0,
    ("discharge", "m³/s"): 1.0,
}


def _lookup_map(pairs: dict) -> Column:
    flat = [x for key, value in pairs.items() for x in ((key if isinstance(key, str) else "|".join(key)), value)]
    return F.create_map(*[F.lit(x) for x in flat])


def standardized_metric(metric_name: Column, value: Column, unit: Column) -> tuple[Column, Column]:
    """Return ``(value, unit)`` with the metric's value in its canonical unit.

    When the metric has a canonical unit and the row's source unit is
    recognised, the value is scaled by the conversion factor and the unit
    becomes the canonical one. Anything else passes through unchanged.

    The ``magnitude`` metric has no canonical physical unit, so it is handled
    by two lookups on the reported scale instead:

    * ``_MW_RULES`` — a scale with a published empirical rule has its value
      turned into the equivalent moment magnitude ``Mw*`` (a ``NULL`` value or
      a scale without a rule leaves the value alone);
    * ``_RELABEL`` — the moment-magnitude family, GDACS's ``M`` and the four
      converted scales are restated ``Mw``.

    ``mh`` and ``mun`` fall in neither map: USGS publishes no equation for
    them, so both the value and the label of such a row are kept as reported.

    The returned unit uses superscript exponents.
    """
    name = F.lower(F.trim(F.coalesce(metric_name, F.lit(""))))
    source = F.lower(F.trim(F.coalesce(unit, F.lit(""))))
    key = F.concat(name, F.lit("|"), source)
    factors = _lookup_map(_FACTORS)
    canonical = _lookup_map(CANONICAL_UNITS)[name]
    known = factors[key].isNotNull()
    reported = value.cast("double")
    out_value = reported * F.coalesce(factors[key], F.lit(1.0))
    threshold = _lookup_map(_MW_RULES)[key]
    out_value = F.when(reported < threshold, (reported + F.lit(_MW_OFFSET)) * F.lit(_MW_SLOPE)).otherwise(out_value)
    out_unit = F.when(known & canonical.isNotNull(), canonical).otherwise(unit)
    out_unit = F.coalesce(_lookup_map(_RELABEL)[key], out_unit)
    return out_value, out_unit
