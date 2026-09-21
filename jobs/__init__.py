from model.enums import JobType
from jobs.registry import JobFactory
from jobs import create_table

JobFactory.register(JobType.CREATE, create_table)
