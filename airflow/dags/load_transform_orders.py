from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import json
import os
from psycopg2.extras import Json

# DAG Configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

EXTRACTED_DIR = "/home/ec2-user/airflow/extracted_data"
TRANSFORMED_DIR = "/home/ec2-user/airflow/transformed_data"

def load_transformed_orders():
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    today_str = datetime.today().strftime("%Y-%m-%d")
    input_csv_path = os.path.join(EXTRACTED_DIR, f"GetOrderData-{today_str}.csv")
    transformed_path = os.path.join(TRANSFORMED_DIR, f"orders_transformed_{today_str}.csv")

    print(f"Processing file: {input_csv_path}")

    df = pd.read_csv(input_csv_path)

    # Normalize column names to lowercase + underscores
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Handle camelCase columns that come from JSON exports
    rename_map = {
        "salelocation": "sale_location",
        "clientid": "client_id",
        "customernumber": "customer_number",
        "ordernumber": "order_number",
        "billingaddress": "billing_address",
        "saledate": "sale_date",
        "ordersource": "order_source",
        "customerclass": "customer_class",
        "lineitems": "line_items",
        "totalshippinghandlingcost": "total_shipping_handling_cost",
        "totaldiscount": "total_discount",
        "totalcost": "total_cost",
        "subtotal": "subtotal",
    }

    # Apply renaming only if column exists
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Validate required columns
    required_cols = [
        "sale_location", "client_id", "customer_number", "order_number",
        "billing_address", "sale_date", "order_source", "customer_class",
        "line_items", "total_shipping_handling_cost", "total_discount",
        "total_cost", "subtotal"
    ]

    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"⚠️ Warning: Missing columns in extracted CSV: {missing}")
        # Create empty placeholders for missing columns
        for col in missing:
            df[col] = None

    # Reorder columns for consistent insertion
    df = df[required_cols]

    # Save cleaned CSV before loading
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)
    df.to_csv(transformed_path, index=False)
    print(f"Transformed data saved to: {transformed_path}")

    # SQL insert
    insert_query = """
        INSERT INTO orders (
            sale_location, client_id, customer_number, order_number,
            billing_address, sale_date, order_source, customer_class,
            line_items, total_shipping_handling_cost, total_discount,
            total_cost, subtotal
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    success_count, fail_count = 0, 0

    for _, row in df.iterrows():
        try:
            billing_json = json.loads(row["billing_address"]) if isinstance(row["billing_address"], str) else row["billing_address"]
            line_items_json = json.loads(row["line_items"]) if isinstance(row["line_items"], str) else row["line_items"]

            values = (
                row["sale_location"],
                int(row["client_id"]) if pd.notna(row["client_id"]) else None,
                int(row["customer_number"]) if pd.notna(row["customer_number"]) else None,
                int(row["order_number"]) if pd.notna(row["order_number"]) else None,
                Json(billing_json),
                row["sale_date"],
                row["order_source"],
                row["customer_class"],
                Json(line_items_json),
                row["total_shipping_handling_cost"],
                row["total_discount"],
                row["total_cost"],
                row["subtotal"]
            )

            cur.execute(insert_query, values)
            conn.commit()
            success_count += 1
        except Exception as e:
            print(f"\n❌ Error inserting row (order_number={row.get('order_number')}): {e}")
            conn.rollback()
            fail_count += 1

    cur.close()
    conn.close()
    print(f"✅ Load complete: {success_count} inserted, {fail_count} failed.")

# DAG Definition
with DAG(
    dag_id="load_transformed_orders",
    default_args=default_args,
    schedule_interval=None,  # manual run only
    catchup=False,
    description="Transform and load Order data into PostgreSQL orders table",
) as dag:

    load_task = PythonOperator(
        task_id="load_transformed_orders",
        python_callable=load_transformed_orders,
        dag=dag
    )
