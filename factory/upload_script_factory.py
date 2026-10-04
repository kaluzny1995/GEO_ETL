"""Upload script registry mapping table names to their script instances."""

from typing import ClassVar

from ingestion.upload.upload_script import UploadScript


class UploadScriptFactory:
    """Registry mapping raw table names to their :class:`UploadScript` instances.

    Upload scripts are registered at import time via ``jobs/__init__.py``.
    """

    _registry: ClassVar[dict[str, UploadScript]] = {}

    @classmethod
    def register(cls, table_name: str, script: UploadScript) -> None:
        """Register a table name with its upload script.

        Args:
            table_name: Raw table name (e.g. ``"avalanche_org_raw"``).
            script: Script instance exposing ``upload(spark, config, db_config, ...)``.
        """
        cls._registry[table_name] = script

    @classmethod
    def get(cls, table_name: str) -> UploadScript:
        """Retrieve the upload script for a table name.

        Args:
            table_name: Raw table name.

        Returns:
            The registered script instance.

        Raises:
            ValueError: If no script is registered for *table_name*.
        """
        if table_name not in cls._registry:
            raise ValueError(f"No upload script for '{table_name}'. Available: {list(cls._registry.keys())}")
        return cls._registry[table_name]

    @classmethod
    def tables(cls) -> list[str]:
        """Return a list of all registered table names."""
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Remove all registered scripts from the registry."""
        cls._registry.clear()
