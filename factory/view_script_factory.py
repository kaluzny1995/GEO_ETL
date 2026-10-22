"""View script registry mapping view names to their script instances."""

from typing import ClassVar

from views.view_script import ViewScript


class ViewScriptFactory:
    """Registry mapping view names to their :class:`ViewScript` instances.

    Scripts live in ``views/`` and are registered at import time via
    ``jobs/__init__.py``, keyed by the view they define (e.g.
    ``views/union_vw.py`` → ``"union_vw"``, matching the script's file stem
    and the ``target_view`` config entry).
    """

    _registry: ClassVar[dict[str, ViewScript]] = {}

    @classmethod
    def register(cls, view_name: str, script: ViewScript) -> None:
        """Register a view script.

        Args:
            view_name: View name used on the CLI (file stem, e.g. ``"union_vw"``).
            script: Script instance exposing ``run(spark, config, db_config, ...)``.
        """
        cls._registry[view_name] = script

    @classmethod
    def get(cls, view_name: str) -> ViewScript:
        """Retrieve the script instance for a view name.

        Args:
            view_name: View name used on the CLI.

        Returns:
            The registered script instance.

        Raises:
            ValueError: If no script is registered for *view_name*.
        """
        if view_name not in cls._registry:
            raise ValueError(f"No view script for '{view_name}'. Available: {list(cls._registry.keys())}")
        return cls._registry[view_name]

    @classmethod
    def scripts(cls) -> list[str]:
        """Return a list of all registered view names."""
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Remove all registered scripts from the registry."""
        cls._registry.clear()
