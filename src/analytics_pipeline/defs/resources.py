from dagster_duckdb import DuckDBResource
import dagster as dg
from dagster_dlt import DagsterDltResource

database_resource = DuckDBResource(database="/tmp/jaffle_platform.duckdb")
dlt_resource = DagsterDltResource

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
        }
    )