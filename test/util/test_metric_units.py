import pytest
from pyspark.sql import functions as F

from utils.metric_units import standardized_metric

CASES = [
    # wind speed: knots -> km/h
    ("wind speed", 35.0, "kts", 64.82, "km/h"),
    ("wind speed", 155.0, "KTS", 287.06, "km/h"),
    ("wind speed", 72.0, "km/h", 72.0, "km/h"),  # already canonical
    # area: acres / ha / km2 -> km²
    ("area", 500.0, "acres", 2.0234282112, "km²"),
    ("area", 317489.0, "ha", 3174.89, "km²"),
    ("area", 2649772.0, "km2", 2649772.0, "km²"),  # only the label changes
    # discharge: ft3/s -> m³/s
    ("discharge", 4470.0, "ft3/s", 126.57630426624, "m³/s"),
    ("discharge", 77982.05, "m³/s", 77982.05, "m³/s"),  # already canonical
]


class TestStandardizedMetric:
    """``standardized_metric`` converts a metric's value to its canonical unit.

    The (name, value, unit) triples below are evaluated with a real Spark
    session so the returned Column expressions are actually computed.
    """

    @pytest.mark.parametrize(("name", "value", "unit", "expected_value", "expected_unit"), CASES)
    def test_value_is_converted_to_the_canonical_unit(
        self, spark_session, name, value, unit, expected_value, expected_unit
    ):
        (row,) = (
            spark_session.createDataFrame([(name, value, unit)], "metric_name string, value double, unit string")
            .select(*standardized_metric(F.col("metric_name"), F.col("value"), F.col("unit")))
            .collect()
        )

        assert row[0] == pytest.approx(expected_value)
        assert row[1] == expected_unit

    @pytest.mark.parametrize(
        ("name", "value", "unit"),
        [
            ("magnitude", 0.12, "mh"),  # USGS publishes no equation for Mh
            ("magnitude", 0.0, "mun"),  # "unknown magnitude", not a USGS scale
            ("vei", 2.0, "VEI"),  # a dimensionless index
            ("surface", 700.0, "N/m²"),  # single unit, already superscript
            ("size", None, "destructive size class"),
        ],
    )
    def test_non_convertible_metrics_pass_through_unchanged(self, spark_session, name, value, unit):
        (row,) = (
            spark_session.createDataFrame([(name, value, unit)], "metric_name string, value double, unit string")
            .select(*standardized_metric(F.col("metric_name"), F.col("value"), F.col("unit")))
            .collect()
        )

        assert row[0] == value
        assert row[1] == unit

    @pytest.mark.parametrize(
        ("source_unit", "value", "expected_value"),
        [
            ("ml", 1.42, 1.9564),  # ML below 4.0: 0.67 * (1.42 + 1.5)
            ("ml", 4.0, 4.0),  # the reported value stands from 4.0 on
            ("ml", 5.3, 5.3),
            ("md", 1.33, 1.8961),  # MD follows the ML rule
            ("md", 3.99, 3.6783),  # still below 4.0
            ("mb", 2.0, 2.345),  # mb below 3.0
            ("mb", 3.0, 3.0),  # the reported value stands from 3.0 on
            ("mb", 4.5, 4.5),  # every mb we load is >= 3.0, so it only relabels
            ("mb_lg", 2.5, 2.68),  # mb_Lg is the Nuttli Mn scale (3.0)
            ("mb_lg", 3.0, 3.0),
        ],
    )
    def test_usgs_empirical_rule_produces_the_equivalent_moment_magnitude(
        self, spark_session, source_unit, value, expected_value
    ):
        """USGS OF 1997/464 turns a local/body/duration/Lg report into ``Mw*``.

        ``Mw* = 0.67 * (x + 1.5)`` while ``x`` is below the scale's threshold
        (4.0 for ``ML``/``MD``, 3.0 for ``mb``/``Mn``) and ``x`` itself above
        it; either way the label is restated ``Mw``.
        """
        (row,) = (
            spark_session.createDataFrame(
                [("magnitude", value, source_unit)], "metric_name string, value double, unit string"
            )
            .select(*standardized_metric(F.col("metric_name"), F.col("value"), F.col("unit")))
            .collect()
        )

        assert row[0] == pytest.approx(expected_value)
        assert row[1] == "Mw"

    @pytest.mark.parametrize(
        ("value", "source_unit"),
        [
            (5.3, "mw"),
            (5.3, "MWW"),
            (4.6, "mwr"),
            (4.5, "M"),  # GDACS reports its earthquake magnitude as "M"
        ],
    )
    def test_moment_magnitudes_are_relabelled_to_Mw_without_scaling(self, spark_session, value, source_unit):
        """The moment-magnitude family (and GDACS's ``M``) restates as ``Mw``.

        Magnitude scales are not inter-convertible, so the value is left
        untouched — only the scale label is unified to the standard ``Mw``.
        """
        (row,) = (
            spark_session.createDataFrame(
                [("magnitude", value, source_unit)], "metric_name string, value double, unit string"
            )
            .select(*standardized_metric(F.col("metric_name"), F.col("value"), F.col("unit")))
            .collect()
        )

        assert row[0] == value
        assert row[1] == "Mw"

    def test_unrecognised_source_unit_is_left_alone(self, spark_session):
        """An unknown unit must not be silently relabelled as the canonical one."""
        (row,) = (
            spark_session.createDataFrame([("area", 10.0, "furlongs")], "metric_name string, value double, unit string")
            .select(*standardized_metric(F.col("metric_name"), F.col("value"), F.col("unit")))
            .collect()
        )

        assert row[0] == 10.0
        assert row[1] == "furlongs"

    def test_null_metric_name_keeps_value_and_unit(self, spark_session):
        (row,) = (
            spark_session.createDataFrame([(None, 5.0, "kts")], "metric_name string, value double, unit string")
            .select(*standardized_metric(F.col("metric_name"), F.col("value"), F.col("unit")))
            .collect()
        )

        assert row[0] == 5.0
        assert row[1] == "kts"

    def test_null_value_stays_null_through_the_conversion(self, spark_session):
        (row,) = (
            spark_session.createDataFrame(
                [("wind speed", None, "kts")], "metric_name string, value double, unit string"
            )
            .select(*standardized_metric(F.col("metric_name"), F.col("value"), F.col("unit")))
            .collect()
        )

        assert row[0] is None
        assert row[1] == "km/h"
