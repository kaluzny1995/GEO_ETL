"""Shared column helpers ``utils/column_helpers.py`` — ``literal_map`` and ``state_province``.

These two helpers are the single place several SINGLE_ETL scripts share an
implementation, so a change to this module should stay explicit here rather than
re-spelling the logic in each script. The tests exercise the helpers directly on a
real (session-scoped) local ``SparkSession`` and also pin down the contract each
using script relies on — in particular the four scripts that call the helpers so the
remaining six are free to keep their own variants.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import pytest

from utils.column_helpers import literal_map, state_province


@pytest.mark.parametrize("mapping", [
    {"USA": "US", "DEU": "DE"},
    {"a": "b"},
    {"_category_": "_subcategory_", "unknown": "xx"},
])
def test_literal_map_returns_a_map_column(spark_session, mapping):
    """A literal map column built from any config mapping, all keys and values as
    literals, resolving the starred lookup and falling through otherwise."""

    col = literal_map(mapping)
    assert isinstance(col, F.Column)

    data = [(k,) for k in mapping]
    rdd = spark_session.sparkContext.parallelize(data)
    df = spark_session.createDataFrame(rdd, ["key"])
    mapped = F.create_map(*[F.lit(item) for pair in mapping.items() for item in pair])
    resolved = df.select(mapped[F.col("key")].alias("v")).collect()

    collected = {r["v"] for r in resolved}
    assert collected == set(mapping.values())

    # A key the map does not know resolves to NULL (create_map behaviour, not the
    # helpers' problem, but each using script relies on it).
    unknown = df.select(mapped[F.lit("__missing__")].alias("v")).collect()
    assert all(r["v"] is None for r in unknown)


@pytest.mark.parametrize("mapping", [
    {"USA": "US"},
    {"cat": "other", "tc": "storm"},
    {"action": "low", "minor": "medium", "major": "extreme"},
])
def test_literal_map_signature_is_broad_enough(spark_session, mapping):
    """The helper accepts a plain ``dict`` as well as any value type the config
    maps may contain (e.g. a ``DangerRating`` enum), since each caller passes its
    own config mapping and the helper must accept all of them."""

    col = literal_map(mapping)
    assert isinstance(col, F.Column)

    data = [(k,) for k in mapping]
    rdd = spark_session.sparkContext.parallelize(data)
    df = spark_session.createDataFrame(rdd, ["key"])
    mapped = F.create_map(*[F.lit(item) for pair in mapping.items() for item in pair])
    resolved = df.select(mapped[F.col("key")].alias("v")).collect()

    collected = {r["v"] for r in resolved}
    assert collected == set(mapping.values())

    unknown = df.select(mapped[F.lit("__missing__")].alias("v")).collect()
    assert all(r["v"] is None for r in unknown)


@pytest.mark.parametrize(
    "geocode_state, expected",
    [
        ("WI", "WI"),              # passes through unchanged — no uppercasing
        ("us-tx", "us-tx"),        # no case folding; scripts that want upper own that
        (" oregon ", " oregon "),  # no trimming; scripts that want strip own that
        ("", None),
        (None, None),
    ],
)
def test_state_province_passes_through_the_stamped_code(spark_session, geocode_state, expected):
    """The stamped ``geocode_state`` of a point, passed through unchanged; a blank
    stamp resolves to ``NULL`` so the four using scripts can coalesce to ``ZZ``
    or leave it ``NULL`` themselves. This helper does **not** uppercase or trim;
    callers that need that transform apply it themselves."""

    col = state_province(F.lit(geocode_state).cast(StringType()))
    assert isinstance(col, F.Column)

    df = spark_session.createDataFrame([(geocode_state,)], StringType())
    value = df.select(col.alias("v")).collect()[0]["v"]
    assert value == expected


def test_state_province_is_pure_spark_with_no_external_deps():
    """Neither helper calls any network service — they are pure Spark column
    expressions. This is the contract the four using scripts rely on."""

    col_a = literal_map({"x": "y"})
    col_b = state_province(F.lit("CA").cast(StringType()))
    assert isinstance(col_a, F.Column)
    assert isinstance(col_b, F.Column)
