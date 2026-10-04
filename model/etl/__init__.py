"""Canonical ETL models — every ``*_etl`` table shares the same event structure.

The models here are written by the ``SINGLE_ETL`` scripts in ``single_etl/``
and are meant to be concatenated in the next pipeline stage, so field order,
types and meanings are identical for every data source.
"""
