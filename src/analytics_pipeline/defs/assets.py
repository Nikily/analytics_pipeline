import dagster as dg
import duckdb
from dagster_duckdb import DuckDBResource



def import_url_to_duckdb(url: str, duckdb: DuckDBResource, table_name: str):
    with duckdb.get_connection() as conn:        
        row_count = conn.execute( 
            f"""
            create or replace table {table_name} as (
            select * from read_csv_auto('{url}')
            )
            """
        ).fetchone()
        assert row_count is not None
        row_count = row_count[0]
        
@dg.asset(kinds={"duckdb"}, key=["target", "main", "raw_customers"])
def raw_customers(duckdb: DuckDBResource) -> None:
    import_url_to_duckdb(
        url="https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv",
        duckdb=duckdb,
        table_name="jaffle_platform.main.raw_customers",
    )

@dg.asset(kinds={"duckdb"}, key=["target", "main", "raw_orders"])
def raw_orders(duckdb: DuckDBResource) -> None:
    import_url_to_duckdb(
        url="https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_orders.csv",
        duckdb=duckdb,
        table_name="jaffle_platform.main.raw_orders",
    )

@dg.asset(kinds={"duckdb"}, key=["target", "main", "raw_payments"])
def raw_payments(duckdb: DuckDBResource) -> None:
    import_url_to_duckdb(
        url="https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_payments.csv",
        duckdb=duckdb,
        table_name="jaffle_platform.main.raw_payments",
    )

@dg.asset_check(
    asset=raw_customers,
    description="Check if there are any nulls",
)
def missing_dimension_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    table_name = "jaffle_platform.main.raw_customers"
    
    with duckdb.get_connection() as conn:
        query_result = conn.execute(
            f"""
            select count(*)
            from {table_name}
            where id is null
            """
        ).fetchone()
        count = query_result[0] if query_result else 0
        return dg.AssetCheckResult(
            passed=count == 0, metadata={"customer_id is null": count}
        )