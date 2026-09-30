"""Single ETL script registry mapping script names to their run functions."""

from collections.abc import Callable
from typing import ClassVar

SingleEtlFn = Callable[..., str]
"""Signature shared by every script in ``single_etl/``."""


class SingleEtlFactory:
    """Registry mapping single ETL script names to their ``run()`` callables.

    Scripts live in ``single_etl/`` and are registered at import time via
    ``jobs/__init__.py``, keyed by the script's file name without the ``.py``
    extension (e.g. ``single_etl/template_etl.py`` → ``"template_etl"``).
    """

    _registry: ClassVar[dict[str, SingleEtlFn]] = {}

    @classmethod
    def register(cls, script_name: str, etl_fn: SingleEtlFn) -> None:
        """Register a single ETL script.

        Args:
            script_name: Script name used on the CLI (file stem, e.g. ``"template_etl"``).
            etl_fn: Callable with the single ETL script signature.
        """
        cls._registry[script_name] = etl_fn

    @classmethod
    def get(cls, script_name: str) -> SingleEtlFn:
        """Retrieve the run function for a script name.

        Args:
            script_name: Script name used on the CLI.

        Returns:
            The registered script callable.

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
