"""Single ETL script registry mapping script names to their script instances."""

from typing import ClassVar

from single_etl.single_etl_script import SingleEtlScript


class SingleEtlFactory:
    """Registry mapping single ETL script names to their :class:`SingleEtlScript` instances.

    Scripts live in ``single_etl/`` and are registered at import time via
    ``jobs/__init__.py``, keyed by the script's file name without the ``.py``
    extension (e.g. ``single_etl/avalanche_org_etl.py`` → ``"avalanche_org_etl"``).
    """

    _registry: ClassVar[dict[str, SingleEtlScript]] = {}

    @classmethod
    def register(cls, script_name: str, script: SingleEtlScript) -> None:
        """Register a single ETL script.

        Args:
            script_name: Script name used on the CLI (file stem, e.g. ``"avalanche_org_etl"``).
            script: Script instance exposing ``run(spark, config, db_config, ...)``.
        """
        cls._registry[script_name] = script

    @classmethod
    def get(cls, script_name: str) -> SingleEtlScript:
        """Retrieve the script instance for a script name.

        Args:
            script_name: Script name used on the CLI.

        Returns:
            The registered script instance.

        Raises:
            ValueError: If no script is registered for *script_name*.
        """
        if script_name not in cls._registry:
            raise ValueError(f"No single ETL script for '{script_name}'. Available: {list(cls._registry.keys())}")
        return cls._registry[script_name]

    @classmethod
    def scripts(cls) -> list[str]:
        """Return a list of all registered script names."""
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Remove all registered scripts from the registry."""
        cls._registry.clear()
