from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

# ------------------------------
# Configuration
# ------------------------------
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflowpass"
DB_HOST = "localhost"
DB_PORT = "5432"

TRANSFORMED_DIR = "/home/ec2-user/airflow/transformed_data"

# ------------------------------
# Helper: Load transformed data into Postgres
# ------------------------------
def load_transformed_to_postgres():
    # Find the most recent transformed file
    files = [f for f in os.listdir(TRANSFORMED_DIR) if f.startswith("TransformedCatalog")]
    if not files:
        raise FileNotFoundError("No transformed catalog files found in /transformed_data/")
    
    latest_file = max(
        files, key=lambda x: os.path.getmtime(os.path.join(TRANSFORMED_DIR, x))
    )
    file_path = os.path.join(TRANSFORMED_DIR, latest_file)
    print(f"📦 Loading file: {file_path}")

    # Read CSV
    df = pd.read_csv(file_path)
    print(f"✅ Read {len(df)} rows from transformed catalog file")

    # Connect to Postgres
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()

    # Truncate table (optional – safe if catalog changes daily)
    cur.execute("TRUNCATE TABLE catalog;")

    # Insert each row
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO catalog (opsku, title, retail_price, product_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (opsku)
            DO UPDATE SET
                title = EXCLUDED.title,
                retail_price = EXCLUDED.retail_price,
                product_type = EXCLUDED.product_type;
            """,
            (row["opsku"], row["title"], row["retail_price"], row["product_type"]),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Loaded {len(df)} records into Postgres 'catalog' table.")


# ------------------------------
# DAG Definition
# ------------------------------
with DAG(
    dag_id="load_transformed_catalog",
    start_date=datetime(2025, 10, 4),
    schedule=None,
    catchup=False,
    description="Load transformed catalog CSV into Postgres",
) as dag:

    load_task = PythonOperator(
        task_id="load_catalog_to_postgres",
        python_callable=load_transformed_to_postgres,
    )

load_task
