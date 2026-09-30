"""Upload script registry mapping table names to their upload functions."""

from collections.abc import Callable
from typing import ClassVar


class UploadScriptFactory:
    """Registry mapping raw table names to their upload callables.

    Upload functions are registered at import time via ``jobs/__init__.py``.
    """

    _registry: ClassVar[dict[str, Callable[..., bool]]] = {}

    @classmethod
    def register(cls, table_name: str, upload_fn: Callable[..., bool]) -> None:
        """Register a table name with its upload function.

        Args:
            table_name: Raw table name (e.g. ``"avalanche_org_raw"``).
            upload_fn: Callable with the upload function signature.
        """
        cls._registry[table_name] = upload_fn

    @classmethod
    def get(cls, table_name: str) -> Callable[..., bool]:
        """Retrieve the upload function for a table name.

        Args:
            table_name: Raw table name.

        Returns:
            The registered upload callable.

        Raises:
            ValueError: If no function is registered for *table_name*.
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
