"""Raw-to-plain script registry mapping table names to their transform functions."""

from collections.abc import Callable


class Raw2PlainScriptFactory:
    """Registry mapping plain table names to their raw2plain callables.

    Raw2plain functions are registered at import time via ``jobs/__init__.py``.
    """
    _registry: dict[str, Callable[..., None]] = {}

    @classmethod
    def register(cls, table_name: str, raw2plain_fn: Callable[..., None]) -> None:
        """Register a table name with its raw2plain function.

        Args:
            table_name: Plain table name (e.g. ``"avalanche_org"``).
            raw2plain_fn: Callable with the raw2plain function signature.
        """
        cls._registry[table_name] = raw2plain_fn

    @classmethod
    def get(cls, table_name: str) -> Callable[..., None]:
        """Retrieve the raw2plain function for a table name.

        Args:
            table_name: Plain table name.

        Returns:
            The registered raw2plain callable.

        Raises:
            ValueError: If no function is registered for *table_name*.
        """
        if table_name not in cls._registry:
            raise ValueError(
                f"No raw2plain script for '{table_name}'. "
                f"Available: {list(cls._registry.keys())}"
            )
        return cls._registry[table_name]

    @classmethod
    def tables(cls) -> list[str]:
        """Return a list of all registered table names."""
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Remove all registered scripts from the registry."""
        cls._registry.clear()
