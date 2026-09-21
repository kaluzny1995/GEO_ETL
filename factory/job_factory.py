from model.enums import JobType


class JobFactory:
    """Registry mapping job types to their execution modules."""
    _registry: dict[JobType, object] = {}

    @classmethod
    def register(cls, job_type: JobType, job_module: object) -> None:
        """Register a job type with its module."""
        cls._registry[job_type] = job_module

    @classmethod
    def create(cls, job_type: JobType) -> object:
        """Get module for a job type."""
        if job_type not in cls._registry:
            raise ValueError(f"No job registered for: {job_type}")
        return cls._registry[job_type]

    @classmethod
    def clear(cls) -> None:
        """Clear all registered jobs."""
        cls._registry.clear()
