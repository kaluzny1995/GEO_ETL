"""Job type registry mapping :class:`~model.enums.JobType` values to execution modules."""

import types
from typing import ClassVar

from model.enums import JobType


class JobFactory:
    """Registry mapping job types to their execution modules.

    Modules are registered at import time via ``jobs/__init__.py`` and
    looked up by :class:`JobType` enum value.
    """

    _registry: ClassVar[dict[JobType, types.ModuleType]] = {}

    @classmethod
    def register(cls, job_type: JobType, job_module: types.ModuleType) -> None:
        """Register a job type with its execution module.

        Args:
            job_type: The enum value identifying the job.
            job_module: The module containing a ``run()`` function.
        """
        cls._registry[job_type] = job_module

    @classmethod
    def create(cls, job_type: JobType) -> types.ModuleType:
        """Retrieve the execution module for a job type.

        Args:
            job_type: The enum value identifying the job.

        Returns:
            The registered module.

        Raises:
            ValueError: If no module is registered for *job_type*.
        """
        if job_type not in cls._registry:
            raise ValueError(f"No job registered for: {job_type}")
        return cls._registry[job_type]

    @classmethod
    def clear(cls) -> None:
        """Remove all registered jobs from the registry."""
        cls._registry.clear()
