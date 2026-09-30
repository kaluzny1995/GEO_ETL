"""Standalone single-table PySpark ETL scripts, run by the ``SINGLE_ETL`` job.

Each script is an independent end-to-end pipeline (read → transform → write)
for one dataset and is registered in :class:`factory.SingleEtlFactory` by
``jobs/__init__.py``. ``single_etl/template_etl.py`` is the starting point for
new scripts.
"""
