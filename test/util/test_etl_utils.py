import datetime as dt

from model.enums import LoadType
from utils.etl_utils import resolve_load_type


class TestResolveLoadType:
    def test_auto_empty_target_is_historical(self):
        assert resolve_load_type(None, None) is LoadType.HISTORICAL

    def test_auto_populated_target_is_incremental(self):
        assert resolve_load_type(dt.date(2026, 9, 28), None) is LoadType.INCREMENTAL

    def test_forced_historical_beats_populated_target(self):
        assert resolve_load_type(dt.date(2026, 9, 28), LoadType.HISTORICAL) is LoadType.HISTORICAL

    def test_forced_incremental_on_empty_target_falls_back_to_historical(self):
        assert resolve_load_type(None, LoadType.INCREMENTAL) is LoadType.HISTORICAL
