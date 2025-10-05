from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os
import re
import json
import csv

# Airflow + AWS Config
AWS_CONN_ID = "aws_default"
BUCKET_NAME = "daen-kings-ransom"
S3_PREFIX = "WineryData/"
OUTPUT_DIR = "/home/ec2-user/airflow/extracted_data"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),  # start date in the past
    "retries": 1,
}


def extract_catalog_from_s3():
    """Extract product catalog JSONs from S3 and save as CSV"""
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=S3_PREFIX)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    today_str = datetime.today().strftime("%Y-%m-%d")
    output_csv_path = os.path.join(OUTPUT_DIR, f"GetCatalog-{today_str}.csv")

    all_data = []
    print(f"Output file will be: {output_csv_path}")

    for key in keys:
        filename = os.path.basename(key)

        # Match only GetCatalog-<clientid>-<date>.json
        if not re.match(r"GetCatalog-(\d+)-\d{4}-\d{2}-\d{2}\.json", filename):
            continue

        # Download object from S3
        obj = hook.read_key(key, bucket_name=BUCKET_NAME)
        data = json.loads(obj)

        # Assume data is a list of products
        for item in data:
            all_data.append({
                "opsku": item.get("opsku"),
                "title": item.get("title"),
                "retailPrice": item.get("retailPrice"),
                "productType": item.get("productType"),
            })

    # Save to CSV
    with open(output_csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["opsku", "title", "retailPrice", "productType"])
        writer.writeheader()
        writer.writerows(all_data)

    print(f"Extracted data saved to {output_csv_path}")


# Define DAG
with DAG(
    dag_id="extract_catalog_products",
    default_args=default_args,
    description="Extract product catalog JSON files from S3 and save as CSV",
    schedule_interval=None,   # manual trigger
    catchup=False,
    tags=["catalog", "s3"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_catalog_from_s3",
        python_callable=extract_catalog_from_s3,
    )

    extract_task
