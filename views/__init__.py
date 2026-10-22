"""Standalone view-defining PySpark scripts, run by the ``VIEW`` job.

Each script subclasses :class:`~views.view_script.ViewScript` and defines one
``*_vw`` view over a ``*_etl`` table from a query kept in ``config.json``,
registered in :class:`factory.ViewScriptFactory` by ``jobs/__init__.py``.
"""
