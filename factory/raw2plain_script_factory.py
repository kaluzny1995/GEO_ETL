class Raw2PlainScriptFactory:
    """Registry mapping table names to their raw2plain functions."""
    _registry: dict[str, callable] = {}

    @classmethod
    def register(cls, table_name: str, raw2plain_fn: callable) -> None:
        """Register a table name with its raw2plain function."""
        cls._registry[table_name] = raw2plain_fn

    @classmethod
    def get(cls, table_name: str) -> callable:
        """Get raw2plain function for a table name."""
        if table_name not in cls._registry:
            raise ValueError(
                f"No raw2plain script for '{table_name}'. "
                f"Available: {list(cls._registry.keys())}"
            )
        return cls._registry[table_name]

    @classmethod
    def tables(cls) -> list[str]:
        """Return list of registered table names."""
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered scripts."""
        cls._registry.clear()
