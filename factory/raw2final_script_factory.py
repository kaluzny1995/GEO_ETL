class Raw2FinalScriptFactory:
    """Registry mapping table names to their raw2final functions."""
    _registry: dict[str, callable] = {}

    @classmethod
    def register(cls, table_name: str, raw2final_fn: callable) -> None:
        """Register a table name with its raw2final function."""
        cls._registry[table_name] = raw2final_fn

    @classmethod
    def get(cls, table_name: str) -> callable:
        """Get raw2final function for a table name."""
        if table_name not in cls._registry:
            raise ValueError(
                f"No raw2final script for '{table_name}'. "
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
