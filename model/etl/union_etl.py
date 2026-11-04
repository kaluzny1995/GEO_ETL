"""``geo.union_etl`` — every ``*_etl`` table stacked into one table.

The union table has exactly the canonical 21 columns of
``model/etl/event_etl.py``: the ``union_etl`` script (a regular
``SINGLE_ETL`` script) appends the rows of the nine source ``*_etl`` tables
with ``unionByName``, so a row keeps every value its own script produced —
``source_name`` says which source it came from along with that source's
``event_category``, ``metric_name`` / ``metric_unit`` and ``other_metrics``.
Nothing is remapped or merged: the table is a vertical union, not a
harmonisation of the categories and metrics of the individual sources.

``source_name`` doubles as the per-source incremental key, so each of the
nine tables must carry a distinct label: ``AVALANCHE_ORG``, ``EONET``,
``GDACS``, ``IBTRACS``, ``NIFC``, ``OPENMETEO``, ``USGS_EARTHQUAKE``,
``USGS_NWIS`` and ``USGS_VOLCANO`` (the three USGS tables used to share
one ``USGS`` label — that is why they were renamed). With unique labels,
an ``INCREMENTAL`` run can threshold each source against the target rows
carrying its own ``source_name``: a source that catches up after an outage
still delivers its older files, which a single global ``max(file_date)``
would skip.

Load behaviour (HISTORICAL / INCREMENTAL, auto-detected from the target's
latest ``file_date``) is documented with the script in
``docs/etl_jobs/single-etl/union-etl.md``.
"""

from model.etl.event_etl import EventEtl


class UnionEtl(EventEtl):
    """One row of any source ``*_etl`` table, appended unchanged into ``geo.union_etl``.

    Fields are inherited from :class:`~model.etl.event_etl.EventEtl` — the
    union adds no column of its own, because ``source_name`` already says
    which of the nine tables (after their rename: one distinct label each)
    a row came from. The DDL matches the nine ``*_etl`` tables column for
    column, so ``unionByName`` can stack them.
    """
