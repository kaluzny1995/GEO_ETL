from pydantic import BaseModel

from model.config import AppConfig, SupabaseConnectionConfig
from model.raw import AvalancheOrgRaw
from utils.ddl_generator import pydantic_to_ddl, create_schema, execute_ddl

TABLE_REGISTRY: dict[str, type[BaseModel]] = {
    "avalanche_org_raw": AvalancheOrgRaw,
}


def run(spark, table_name: str) -> str:
    # Validate table name is registered
    if table_name not in TABLE_REGISTRY:
        raise ValueError(
            f"Unknown table '{table_name}'. "
            f"Available: {list(TABLE_REGISTRY.keys())}"
        )

    # Get Pydantic model for this table
    model = TABLE_REGISTRY[table_name]

    # Load configs (Spark master, DB schema, Supabase credentials)
    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()

    # Build JDBC connection params
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)

    # Step 1: Create schema if it doesn't exist (e.g. "geo")
    schema = app_config.database.db_schema
    create_schema(spark, schema, jdbc_url, properties)

    # Step 2: Generate CREATE TABLE DDL from Pydantic model
    ddl = pydantic_to_ddl(model, table_name, schema=schema)

    # Step 3: Execute DDL against Supabase
    execute_ddl(spark, ddl, jdbc_url, properties)

    return f"Table {schema}.{table_name} created."
