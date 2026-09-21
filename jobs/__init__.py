from model.enums import JobType
from factory import JobFactory
from jobs import create_table

JobFactory.register(JobType.CREATE, create_table)
