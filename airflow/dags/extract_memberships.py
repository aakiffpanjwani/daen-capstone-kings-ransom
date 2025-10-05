from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import json
import os

# AWS + S3 config
S3_BUCKET = "daen-kings-ransom"
S3_PREFIX = "WineryData/"
LOCAL_OUTPUT_DIR = "/home/ec2-user/airflow/extracted_data"

def extract_memberships_from_s3(**context):
    """Extract wine club and membership data from S3 and save as CSV."""
    logical_date = context.get("logical_date") or datetime.utcnow()
    date_str = logical_date.strftime("%Y-%m-%d")

    s3 = boto3.client("s3")
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

    output_file = f"{LOCAL_OUTPUT_DIR}/GetMemberships-{date_str}.csv"
    all_records = []

    print("🚀 Starting membership extraction...")
    print(f"📅 Logical date: {date_str}")
    print(f"📦 Scanning bucket: {S3_BUCKET}/{S3_PREFIX}")

    # List all relevant JSON files
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}GetWineClubsAndMembers-")
    contents = response.get("Contents", [])

    if not contents:
        print("⚠️ No matching JSON files found.")
        return

    for obj in contents:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        print(f"📥 Processing file: {key}")
        s3_object = s3.get_object(Bucket=S3_BUCKET, Key=key)
        file_content = s3_object["Body"].read().decode("utf-8")

        try:
            data = json.loads(file_content)
            # Handle different structures
            if isinstance(data, list):
                all_records.extend(data)
            elif isinstance(data, dict) and "data" in data:
                all_records.extend(data["data"])
            else:
                all_records.append(data)
        except Exception as e:
            print(f"❌ Error parsing {key}: {e}")

    if not all_records:
        print("⚠️ No data extracted from JSON files.")
        return

    df = pd.DataFrame(all_records)
    print(f"✅ Extracted {len(df)} membership records.")
    df.to_csv(output_file, index=False)
    print(f"💾 Saved to: {output_file}")

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="extract_memberships_dag",
    default_args=default_args,
    description="Extracts wine club & membership data from S3 JSON files",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "memberships", "s3"],
) as dag:

    extract_memberships = PythonOperator(
        task_id="extract_memberships_from_s3",
        python_callable=extract_memberships_from_s3,
        provide_context=True,
    )

    extract_memberships
