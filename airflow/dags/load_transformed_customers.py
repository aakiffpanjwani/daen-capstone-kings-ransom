from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

# Default Airflow DAG args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Paths
TRANSFORMED_DIR = '/home/ec2-user/airflow/transformed_data'

# Postgres connection parameters
POSTGRES_CONFIG = {
    'host': 'localhost',
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflowpass',   # ✅ updated password
    'port': 5432
}

def load_customers_to_postgres(execution_date, **context):
    """Loads transformed customers CSV into Postgres."""
    date_str = execution_date.strftime('%Y-%m-%d')
    file_path = os.path.join(TRANSFORMED_DIR, f"TransformedCustomers-{date_str}.csv")

    print(f"📦 Loading transformed customer data from: {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ File not found: {file_path}")

    # Read CSV
    df = pd.read_csv(file_path)
    print(f"✅ Read {len(df)} customer records")

    # Ensure table columns align with Postgres schema
    expected_cols = ['customer_uuid', 'customer_number', 'client_id', 'email', 'first_name', 'last_name']
    df = df[expected_cols]

    # Connect to Postgres
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    # Create table if not exists
    create_table_query = """
        CREATE TABLE IF NOT EXISTS customers (
            customer_uuid TEXT PRIMARY KEY,
            customer_number INT,
            client_id INT,
            email TEXT,
            first_name TEXT,
            last_name TEXT
        );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Insert data with upsert protection
    insert_query = """
        INSERT INTO customers (customer_uuid, customer_number, client_id, email, first_name, last_name)
        VALUES %s
        ON CONFLICT (customer_uuid) DO NOTHING;
    """
    records = df.values.tolist()
    execute_values(cur, insert_query, records)

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Successfully loaded {len(df)} records into Postgres 'customers' table.")

# DAG definition
with DAG(
    dag_id='load_transformed_customers',
    default_args=default_args,
    description='Load transformed customer data into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['customers', 'postgres', 'load']
) as dag:

    load_task = PythonOperator(
        task_id='load_customers_to_postgres',
        python_callable=load_customers_to_postgres,
        provide_context=True
    )

    load_task
