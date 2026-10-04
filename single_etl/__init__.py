"""Standalone single-table PySpark ETL scripts, run by the ``SINGLE_ETL`` job.

Each script subclasses :class:`~single_etl.single_etl_script.SingleEtlScript`
and is an independent end-to-end pipeline (read → transform → write) for one
dataset, registered in :class:`factory.SingleEtlFactory` by
``jobs/__init__.py``. ``single_etl/avalanche_org_etl.py`` is the reference
script — copy it to add the next source.
"""
