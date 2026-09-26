"""Lazy-loading re-exports for factory classes.

Avoids circular imports between ``model/`` and ``factory/`` by deferring
actual imports until attribute access time.
"""


def __getattr__(name: str):
    """Lazily import a factory class by name.

    Args:
        name: Attribute name (e.g. ``"JobFactory"``).

    Returns:
        The requested factory class.

    Raises:
        AttributeError: If *name* is not a known factory class.
    """
    if name == "JobFactory":
        from factory.job_factory import JobFactory

        return JobFactory
    if name == "TableFactory":
        from factory.table_factory import TableFactory

        return TableFactory
    if name == "UploadScriptFactory":
        from factory.upload_script_factory import UploadScriptFactory

        return UploadScriptFactory
    if name == "Raw2PlainScriptFactory":
        from factory.raw2plain_script_factory import Raw2PlainScriptFactory

        return Raw2PlainScriptFactory
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["JobFactory", "Raw2PlainScriptFactory", "TableFactory", "UploadScriptFactory"]
