"""
DAG Task: Load transformed memberships into Postgres
Handles NaT/NaN timestamps safely and inserts cleanly.
"""

import os
import glob
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def load_memberships_to_postgres(**kwargs):
    logging.info("🚀 Starting load for transformed memberships")

    # --- Locate latest transformed file ---
    transformed_dir = "/home/ec2-user/airflow/transformed_data"
    files = sorted(
        glob.glob(os.path.join(transformed_dir, "TransformedMemberships-*.csv")),
        key=os.path.getmtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("No transformed membership CSVs found.")
    latest_file = files[0]
    logging.info(f"📂 Using transformed file: {latest_file}")

    # --- Load Data ---
    df = pd.read_csv(latest_file)
    logging.info(f"📊 Loaded {len(df)} rows from CSV")

    # --- Convert timestamp columns safely ---
    for col in ["signup_date", "cancel_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df[col] = df[col].where(df[col].notnull(), None)

    # --- Clean any lingering invalid values ---
    df = df.replace(["NaT", "NaN", "nan", "", " "], None)

    # --- Deduplicate ---
    df = df.drop_duplicates(subset=["id"])
    logging.info(f"🧩 Deduplicated to {len(df)} unique IDs")

    # --- Connect to Postgres ---
    conn = BaseHook.get_connection("postgres_default")
    pg_conn = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port,
    )
    cur = pg_conn.cursor()

    # --- Truncate for full reload ---
    cur.execute("TRUNCATE TABLE memberships;")
    logging.info("🧹 Cleared existing memberships data.")

    # --- Prepare Insert ---
    insert_query = """
        INSERT INTO memberships (
            club_type, id, signup_date, cancel_date, client_id,
            customer_number, email, first_name, last_name,
            address1, city, state, zip
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    # --- Build clean record tuples ---
    records = []
    for _, row in df.iterrows():
        record = tuple(
            None if (pd.isna(val) or val in ["NaT", "NaN", ""]) else val
            for val in row
        )
        records.append(record)

    logging.info(f"💾 Preparing to insert {len(records)} records...")

    # --- Insert safely ---
    try:
        execute_batch(cur, insert_query, records, page_size=500)
        pg_conn.commit()
        logging.info("✅ Data inserted successfully into Postgres!")
    except Exception as e:
        pg_conn.rollback()
        logging.error(f"❌ Failed to insert data: {e}")
        raise
    finally:
        cur.close()
        pg_conn.close()
        logging.info("🔒 PostgreSQL connection closed.")


# --- DAG Definition ---
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="load_transformed_memberships",
    default_args=default_args,
    start_date=datetime(2025, 10, 5),
    schedule_interval=None,
    catchup=False,
    tags=["ETL", "Postgres", "memberships"],
) as dag:
    load_task = PythonOperator(
        task_id="load_memberships",
        python_callable=load_memberships_to_postgres,
        provide_context=True,
    )
