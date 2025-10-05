from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# ---------- PATH CONFIG ----------
EXTRACTED_DIR = "/home/ec2-user/airflow/extracted_data"
TRANSFORMED_DIR = "/home/ec2-user/airflow/transformed_data"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def transform_catalog():
    """Read latest extracted catalog CSV, clean and standardize fields, save to transformed folder"""
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)

    # Get the most recent extracted file
    csv_files = [f for f in os.listdir(EXTRACTED_DIR) if f.startswith("GetCatalog") and f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("No extracted CSV files found in extracted_data directory.")
    
    latest_file = sorted(csv_files)[-1]
    input_path = os.path.join(EXTRACTED_DIR, latest_file)
    print(f"Processing file: {input_path}")

    df = pd.read_csv(input_path)

    # --- Clean and Transform ---
    df = df.rename(columns={
        'retailPrice': 'retail_price',
        'productType': 'product_type'
    })

    # Handle missing or invalid prices
    df['retail_price'] = pd.to_numeric(df['retail_price'], errors='coerce').fillna(0.0)

    # Fill empty strings for missing titles or product types
    df['title'] = df['title'].fillna('Unknown')
    df['product_type'] = df['product_type'].fillna('Unknown')

    # Drop duplicate rows if any
    df = df.drop_duplicates(subset=['opsku'])

    # Save transformed file
    today_str = datetime.today().strftime("%Y-%m-%d")
    output_path = os.path.join(TRANSFORMED_DIR, f"TransformedCatalog-{today_str}.csv")
    df.to_csv(output_path, index=False)
    print(f"Transformed data saved to: {output_path}")
    print(f"Row count: {len(df)}")

# ---------- DAG DEFINITION ----------
with DAG(
    dag_id="transform_load_catalog",
    default_args=default_args,
    description="Transform extracted catalog data for loading into Postgres",
    schedule_interval=None,
    catchup=False,
    tags=["catalog", "transform"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_catalog_file",
        python_callable=transform_catalog,
    )

    transform_task
