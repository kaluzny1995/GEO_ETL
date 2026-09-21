from model.enums import JobType


class JobFactory:
    _registry: dict[JobType, object] = {}

    @classmethod
    def register(cls, job_type: JobType, job_module: object) -> None:
        cls._registry[job_type] = job_module

    @classmethod
    def create(cls, job_type: JobType) -> object:
        if job_type not in cls._registry:
            raise ValueError(f"No job registered for: {job_type}")
        return cls._registry[job_type]

    @classmethod
    def clear(cls) -> None:
        cls._registry.clear()
