from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import re
import json
import csv
import os

# --- CONFIGURATION ---
AWS_CONN_ID = 'aws_default'
BUCKET_NAME = "daen-kings-ransom"         # your bucket name
S3_PREFIX = "WineryData/"                 # your prefix
OUTPUT_DIR = "/home/ec2-user/airflow/extracted_data"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# --- EXTRACTION FUNCTION ---
def extract_orders_from_s3():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=S3_PREFIX)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    today_str = datetime.today().strftime("%Y-%m-%d")
    output_csv_path = os.path.join(OUTPUT_DIR, f"GetOrderData-{today_str}.csv")

    all_data = []
    print(f"Output file will be: {output_csv_path}")

    for key in keys:
        filename = os.path.basename(key)

        # Only process files that match GetOrderData pattern
        match = re.match(r"GetOrderData-(\d+)-\d{4}-\d{2}-\d{2}\.json", filename)
        if not match:
            continue  # skip non-GetOrderData files

        print(f"✅ Found order file: {filename}")
        client_id = match.group(1)

        try:
            file_content = hook.read_key(key=key, bucket_name=BUCKET_NAME)
            records = json.loads(file_content)
        except Exception as e:
            print(f"⚠️ Error reading/parsing file {key}: {e}")
            continue

        # Process nested order data
        for rec in records:
            for order in rec.get("data", []):
                billing = order.get("billAddress", {})
                line_items = order.get("lineItems", [])

                billing_array = [{
                    "firstName": billing.get("firstName"),
                    "lastName": billing.get("lastName"),
                    "country": billing.get("country"),
                    "birthDate": billing.get("birthDate"),
                    "email": billing.get("email"),
                    "phone": billing.get("phone"),
                    "address1": billing.get("address1"),
                    "city": billing.get("city"),
                    "state": billing.get("state"),
                    "zipCode": billing.get("zipCode"),
                    "address2": billing.get("address2"),
                    "company": billing.get("company")
                }]

                line_items_array = [{
                    "productType": item.get("productType"),
                    "productCategory": item.get("productCategory"),
                    "productTitle": item.get("productTitle"),
                    "opsku": item.get("oPSKU"),
                    "quantity": item.get("qty"),
                    "unitPrice": item.get("unitPrice"),
                    "productDiscount": item.get("totalDiscount"),
                    "extendedCost": item.get("extendedCost"),
                    "productSalesText": item.get("productSalesTax"),
                    "revenue": item.get("revenue"),
                    "netRevenue": item.get("netRevenue")
                } for item in line_items]

                all_data.append({
                    "saleLocation": order.get("saleLocation"),
                    "clientId": client_id,
                    "customerNumber": order.get("customerNumber"),
                    "orderNumber": order.get("orderNumber"),
                    "billingAddress": json.dumps(billing_array),
                    "saleDate": order.get("saleDate"),
                    "orderSource": order.get("orderSource"),
                    "customerClass": order.get("customerClass"),
                    "lineItems": json.dumps(line_items_array),
                    "totalShippingHandlingCost": order.get("totalShipAndHandlingCost"),
                    "totalDiscount": order.get("totalDiscount"),
                    "totalCost": order.get("totalCost"),
                    "subTotal": order.get("subTotal")
                })

    if all_data:
        fieldnames = [
            "saleLocation", "clientId", "customerNumber", "orderNumber", "billingAddress",
            "saleDate", "orderSource", "customerClass", "lineItems",
            "totalShippingHandlingCost", "totalDiscount", "totalCost", "subTotal"
        ]
        try:
            with open(output_csv_path, "w", newline='', encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_data)
            print(f"✅ Extracted data saved to {output_csv_path}")
        except Exception as e:
            print(f"❌ Failed to write CSV file: {e}")
    else:
        print("⚠️ No GetOrderData files found or processed.")

# --- DAG DEFINITION ---
dag = DAG(
    dag_id="extract_orders",
    default_args=default_args,
    schedule_interval=None,   # manual run only
    catchup=False,
    description="Manual DAG: Extract GetOrderData JSONs from daen-kings-ransom/WineryData/",
)

with dag:
    extract_task = PythonOperator(
        task_id="extract_orders_from_s3",
        python_callable=extract_orders_from_s3,
        dag=dag
    )
