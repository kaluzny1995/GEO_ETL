"""Raw-to-plain script registry mapping table names to their script instances."""

from typing import ClassVar

from ingestion.raw2plain.raw2plain_script import Raw2PlainScript


class Raw2PlainScriptFactory:
    """Registry mapping plain table names to their :class:`Raw2PlainScript` instances.

    Raw2plain scripts are registered at import time via ``jobs/__init__.py``.
    """

    _registry: ClassVar[dict[str, Raw2PlainScript]] = {}

    @classmethod
    def register(cls, table_name: str, script: Raw2PlainScript) -> None:
        """Register a table name with its raw2plain script.

        Args:
            table_name: Plain table name (e.g. ``"avalanche_org"``).
            script: Script instance exposing ``raw2plain(spark, config, db_config, ...)``.
        """
        cls._registry[table_name] = script

    @classmethod
    def get(cls, table_name: str) -> Raw2PlainScript:
        """Retrieve the raw2plain script for a table name.

        Args:
            table_name: Plain table name.

        Returns:
            The registered script instance.

        Raises:
            ValueError: If no script is registered for *table_name*.
        """
        if table_name not in cls._registry:
            raise ValueError(f"No raw2plain script for '{table_name}'. Available: {list(cls._registry.keys())}")
        return cls._registry[table_name]

    @classmethod
    def tables(cls) -> list[str]:
        """Return a list of all registered table names."""
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Remove all registered scripts from the registry."""
        cls._registry.clear()
