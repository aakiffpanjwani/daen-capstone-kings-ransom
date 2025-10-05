from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
import json
import io
import os

# --- DAG DEFAULTS ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# --- AWS CONFIG ---
S3_BUCKET = "daen-kings-ransom"
S3_PREFIX = "WineryData/"
OUTPUT_DIR = "/home/ec2-user/airflow/extracted_data/"

# --- FUNCTION ---
def extract_customers_from_s3(**context):
    logical_date = context.get("logical_date")
    date_str = logical_date.strftime("%Y-%m-%d")

    s3 = boto3.client("s3")
    all_records = []

    print("🚀 Starting customer extraction...")
    print(f"📅 Logical date: {date_str}")
    print(f"📦 Scanning S3 bucket: {S3_BUCKET}/{S3_PREFIX}")

    # List all JSON files that start with GetCustomers-
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}GetCustomers-")

    if "Contents" not in response:
        print("⚠️ No customer files found.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        print(f"📥 Processing file: {key}")

        s3_object = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = s3_object["Body"].read().decode("utf-8")

        try:
            json_data = json.loads(content)

            # Handle both list and dict structures
            if isinstance(json_data, dict) and "items" in json_data:
                records = json_data["items"]
            elif isinstance(json_data, list):
                records = json_data
            else:
                records = [json_data]

            for rec in records:
                all_records.append({
                    "customer_uuid": rec.get("customerUuid"),
                    "customer_number": rec.get("customerNumber"),
                    "client_id": rec.get("clientId"),
                    "email": rec.get("email"),
                    "first_name": rec.get("firstName"),
                    "last_name": rec.get("lastName")
                })

        except Exception as e:
            print(f"❌ Error reading {key}: {e}")
            continue

    if not all_records:
        print("⚠️ No customer records extracted.")
        return

    df = pd.DataFrame(all_records)

    # Drop duplicates and clean null UUIDs
    df = df.dropna(subset=["customer_uuid"]).drop_duplicates(subset=["customer_uuid"])
    df.reset_index(drop=True, inplace=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"GetCustomers-{date_str}.csv")

    df.to_csv(output_file, index=False)
    print(f"✅ Extracted {len(df)} customer records to: {output_file}")


# --- DAG SETUP ---
with DAG(
    dag_id="extract_customers_dag",
    default_args=default_args,
    description="Extract customer data from S3 JSON files and save as CSV",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["customers", "extract", "s3"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_customers_from_s3",
        python_callable=extract_customers_from_s3,
        provide_context=True,
    )

    extract_task
