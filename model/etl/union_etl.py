"""``geo.union_etl`` — every ``*_etl`` table stacked into one table.

The union table has exactly the canonical 21 columns of
``model/etl/event_etl.py``: the ``union_etl`` script (a regular
``SINGLE_ETL`` script) appends the rows of the nine source ``*_etl`` tables
with ``unionByName``, so a row keeps every value its own script produced —
``source_name`` says which source it came from (``AVALANCHE_ORG``, ``EONET``,
``GDACS``, …), along with that source's ``event_category``, ``metric_name`` /
``metric_unit`` and ``other_metrics``. Nothing is remapped or merged: the
table is a vertical union, not a harmonisation of the categories and metrics
of the individual sources.

Load behaviour (HISTORICAL / INCREMENTAL, auto-detected from the target's
latest ``file_date``) is documented with the script in
``docs/etl_jobs/single-etl.md#union_etl``.
"""

from model.etl.event_etl import EventEtl


class UnionEtl(EventEtl):
    """One row of any source ``*_etl`` table, appended unchanged into ``geo.union_etl``.

    Fields are inherited from :class:`~model.etl.event_etl.EventEtl` — the
    union adds no column of its own, because the source of a row is already
    carried by ``source_name``. The DDL matches the nine ``*_etl`` tables
    column for column, so ``unionByName`` can stack them.
    """
