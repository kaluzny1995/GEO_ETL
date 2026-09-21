from pydantic import BaseModel


class TableFactory:
    """Registry mapping table names to their Pydantic models."""
    _registry: dict[str, type[BaseModel]] = {}

    @classmethod
    def register(cls, table_name: str, model: type[BaseModel]) -> None:
        """Register a table name with its Pydantic model."""
        cls._registry[table_name] = model

    @classmethod
    def get(cls, table_name: str) -> type[BaseModel]:
        """Get Pydantic model for a table name."""
        if table_name not in cls._registry:
            raise ValueError(
                f"Unknown table '{table_name}'. "
                f"Available: {list(cls._registry.keys())}"
            )
        return cls._registry[table_name]

    @classmethod
    def tables(cls) -> list[str]:
        """Return list of registered table names."""
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered tables."""
        cls._registry.clear()
