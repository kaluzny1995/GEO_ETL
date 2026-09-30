import datetime as dt

import pytest

from utils.fetch_avalanche_zones import (
    _geometry_centroid,
    _lookup_rows,
    _polygon_centroid,
    _ring_centroid,
    build_lookup,
    lookup_days,
)

SQUARE = [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]


class TestRingCentroid:
    def test_unit_square(self):
        lon, lat, area = _ring_centroid(SQUARE)

        assert lon == pytest.approx(0.5)
        assert lat == pytest.approx(0.5)
        assert area == pytest.approx(1.0)

    def test_orientation_does_not_change_the_centroid(self):
        lon, lat, area = _ring_centroid(list(reversed(SQUARE)))

        assert lon == pytest.approx(0.5)
        assert lat == pytest.approx(0.5)
        assert area == pytest.approx(1.0)

    def test_degenerate_ring_falls_back_to_the_vertex_average(self):
        lon, lat, area = _ring_centroid([[0, 0], [2, 0], [4, 0], [0, 0]])

        assert area == 0
        assert (lon, lat) == pytest.approx((2, 0))


class TestPolygonCentroid:
    def test_square_with_a_hole_stays_centred(self):
        outer = [[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]
        hole = [[0.5, 0.5], [1.5, 0.5], [1.5, 1.5], [0.5, 1.5], [0.5, 0.5]]
        lon, lat, area = _polygon_centroid([outer, hole])

        assert lon == pytest.approx(1.0)
        assert lat == pytest.approx(1.0)
        assert area == pytest.approx(3.0)

    def test_holes_canceling_the_polygon_fall_back_to_the_exterior(self):
        outer = [[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]
        hole = [[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]
        lon, lat, area = _polygon_centroid([outer, hole])

        assert area == pytest.approx(4.0)
        assert lon == pytest.approx(1.0)
        assert lat == pytest.approx(1.0)


class TestGeometryCentroid:
    def test_polygon(self):
        lon, lat = _geometry_centroid({"type": "Polygon", "coordinates": [SQUARE]})

        assert (lon, lat) == pytest.approx((0.5, 0.5))

    def test_multipolygon_is_area_weighted(self):
        far = [[3, 0], [4, 0], [4, 1], [3, 1], [3, 0]]
        lon, lat = _geometry_centroid({"type": "MultiPolygon", "coordinates": [[SQUARE], [far]]})

        assert lon == pytest.approx(2.0)
        assert lat == pytest.approx(0.5)

    def test_missing_geometry(self):
        assert _geometry_centroid(None) is None

    def test_empty_geometry(self):
        assert _geometry_centroid({"type": "Polygon", "coordinates": []}) is None

    def test_unsupported_geometry_type(self):
        assert _geometry_centroid({"type": "Point", "coordinates": [1, 2]}) is None


class TestLookupRows:
    @staticmethod
    def _feature(name, state, geometry):
        return {"properties": {"name": name, "state": state}, "geometry": geometry}

    def test_rows_come_from_zone_name_and_state(self):
        feature = self._feature("Tetons", "WY", {"type": "Polygon", "coordinates": [SQUARE]})
        (row,) = _lookup_rows([feature])

        assert row["place_location"] == "Tetons"
        assert row["state"] == "WY"
        assert row["latitude"] == pytest.approx(0.5)
        assert row["longitude"] == pytest.approx(0.5)

    def test_sorted_by_state_then_zone(self):
        rows = _lookup_rows(
            [
                self._feature("Zulu", "CA", {"type": "Polygon", "coordinates": [SQUARE]}),
                self._feature("Alpha", "AK", {"type": "Polygon", "coordinates": [SQUARE]}),
                self._feature("Mike", "CO", {"type": "Polygon", "coordinates": [SQUARE]}),
            ]
        )

        assert [(row["state"], row["place_location"]) for row in rows] == [
            ("AK", "Alpha"),
            ("CA", "Zulu"),
            ("CO", "Mike"),
        ]

    def test_skips_features_without_a_name_or_geometry(self):
        rows = _lookup_rows(
            [
                {"properties": {"name": "", "state": "CA"}, "geometry": {"type": "Polygon", "coordinates": [SQUARE]}},
                self._feature("No Geometry", "CA", None),
                self._feature("Kept", "CA", {"type": "Polygon", "coordinates": [SQUARE]}),
            ]
        )

        assert [row["place_location"] for row in rows] == ["Kept"]

    def test_first_day_wins_for_a_duplicated_zone(self):
        rows = _lookup_rows(
            [
                self._feature("Tetons", "WY", {"type": "Polygon", "coordinates": [SQUARE]}),
                self._feature(
                    "Tetons",
                    "WY",
                    {"type": "Polygon", "coordinates": [[[3, 0], [4, 0], [4, 1], [3, 1], [3, 0]]]},
                ),
            ]
        )

        assert len(rows) == 1
        assert rows[0]["longitude"] == pytest.approx(0.5)


class TestLookupDays:
    def test_window_is_today_then_10_and_30_days_back(self):
        days = lookup_days(today=dt.date(2026, 9, 30))

        assert days == ["2026-09-30", "2026-09-20", "2026-08-31"]

    def test_defaults_to_today(self):
        assert lookup_days()[0] == dt.date.today().isoformat()


class TestBuildLookup:
    @staticmethod
    def _feature(name: str = "Tetons", state: str = "WY"):
        return {"properties": {"name": name, "state": state}, "geometry": {"type": "Polygon", "coordinates": [SQUARE]}}

    def test_writes_the_csv_and_creates_the_directory(self, tmp_path, monkeypatch):
        monkeypatch.setattr("utils.fetch_avalanche_zones._fetch", lambda url, days: [self._feature()])
        output = tmp_path / "cache" / "lookup.csv"

        written = build_lookup(["2026-09-30"], output=str(output))

        assert written == 1
        assert output.read_text().splitlines() == [
            "place_location,state,latitude,longitude",
            "Tetons,WY,0.5,0.5",
        ]

    def test_no_usable_geometry_raises_and_writes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("utils.fetch_avalanche_zones._fetch", lambda url, days: [])
        output = tmp_path / "cache" / "lookup.csv"

        with pytest.raises(RuntimeError, match="No zone geometries"):
            build_lookup(["2026-09-30"], output=str(output))

        assert not output.exists()
