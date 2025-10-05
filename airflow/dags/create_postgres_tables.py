from airflow import DAG
try:
    # New import path (Airflow 2.3+)
    from airflow.providers.postgres.operators.postgres import PostgresOperator
except ModuleNotFoundError:
    # Fallback for older versions
    from airflow.providers.postgres.operators.postgres_operator import PostgresOperator

from datetime import datetime
# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

# Define the DAG
with DAG(
    dag_id="create_postgres_tables",
    default_args=default_args,
    schedule_interval=None,   # Run only when triggered manually
    catchup=False,
    description="Create all the raw tables in Postgres for the project",
) as dag:

    # Task: Create all required tables
    create_all_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_default",   # Connection must exist in Airflow
        sql="""
        CREATE TABLE IF NOT EXISTS catalog (
            opsku TEXT PRIMARY KEY,
            title TEXT,
            retail_price NUMERIC(10,2),
            product_type TEXT
        );

        CREATE TABLE IF NOT EXISTS customers (
            customer_uuid TEXT PRIMARY KEY,
            customer_number INTEGER NOT NULL,
            client_id BIGINT NOT NULL,
            email TEXT,
            first_name TEXT,
            last_name TEXT
        );

        CREATE TABLE IF NOT EXISTS memberships (
            club_type TEXT,
            id INTEGER PRIMARY KEY,
            signup_date TIMESTAMP,
            cancel_date TIMESTAMP,
            client_id BIGINT NOT NULL,
            customer_number INTEGER NOT NULL,
            email TEXT,
            first_name TEXT,
            last_name TEXT,
            address1 TEXT,
            city TEXT,
            state TEXT,
            zip TEXT
        );

        CREATE TABLE IF NOT EXISTS orders (
            sale_location TEXT,
            client_id BIGINT NOT NULL,
            customer_number INTEGER NOT NULL,
            order_number INTEGER NOT NULL,
            billing_address JSONB,
            sale_date TIMESTAMP,
            order_source TEXT,
            customer_class TEXT,
            line_items JSONB,
            total_shipping_handling_cost NUMERIC(10,2),
            total_discount NUMERIC(10,2),
            total_cost NUMERIC(10,2),
            subtotal NUMERIC(10,2),
            PRIMARY KEY (order_number, client_id)
        );
        """
    )
