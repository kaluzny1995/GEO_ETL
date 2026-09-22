class UploadScriptFactory:
    """Registry mapping table names to their upload functions."""
    _registry: dict[str, callable] = {}

    @classmethod
    def register(cls, table_name: str, upload_fn: callable) -> None:
        """Register a table name with its upload function."""
        cls._registry[table_name] = upload_fn

    @classmethod
    def get(cls, table_name: str) -> callable:
        """Get upload function for a table name."""
        if table_name not in cls._registry:
            raise ValueError(
                f"No upload script for '{table_name}'. "
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
