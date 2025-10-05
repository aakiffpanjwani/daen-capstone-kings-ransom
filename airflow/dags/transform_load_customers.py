from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def transform_customers_file(execution_date):
    print("🚀 Starting customer data transformation...")

    # handle execution_date safely
    if isinstance(execution_date, str):
        try:
            execution_date = datetime.fromisoformat(execution_date)
        except ValueError:
            execution_date = datetime.today()

    date_str = execution_date.strftime("%Y-%m-%d")

    extracted_path = f"/home/ec2-user/airflow/extracted_data/GetCustomers-{date_str}.csv"
    transformed_dir = "/home/ec2-user/airflow/transformed_data"
    transformed_path = f"{transformed_dir}/TransformedCustomers-{date_str}.csv"

    os.makedirs(transformed_dir, exist_ok=True)

    print(f"📂 Reading extracted file: {extracted_path}")
    df = pd.read_csv(extracted_path)
    print(f"📊 Original row count: {len(df)}")

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Check presence of UUID column
    if "customer_uuid" not in df.columns:
        raise ValueError("❌ Column 'customer_uuid' not found in the dataset!")

    # Clean UUIDs safely (don’t drop all)
    before = len(df)
    df["customer_uuid"] = df["customer_uuid"].astype(str).str.strip().replace("nan", "")
    df = df[df["customer_uuid"] != ""]
    df = df.drop_duplicates(subset=["customer_uuid"])
    print(f"🧹 Dropped {before - len(df)} invalid or duplicate UUID rows")

    # Print sample of UUIDs for debugging
    print("🔎 Sample UUIDs after cleaning:")
    print(df["customer_uuid"].head(5).to_list())

    # Clean up names and email
    for col in ["email", "first_name", "last_name"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("nan", "")
            )
            if col == "email":
                df[col] = df[col].str.lower()
            else:
                df[col] = df[col].str.title()

    # Convert numeric columns
    for col in ["customer_number", "client_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df.to_csv(transformed_path, index=False)
    print(f"✅ Transformed file saved to: {transformed_path}")
    print(f"✅ Row count after cleaning: {len(df)}")
    print("🎯 Transformation complete!")

with DAG(
    dag_id="transform_customers_dag",
    default_args=default_args,
    description="Transform extracted customer data before loading into Postgres",
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["ETL", "transform", "customers"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_customers_file",
        python_callable=transform_customers_file,
    )

    transform_task
