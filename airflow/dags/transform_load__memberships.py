import os
import ast
import logging
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# -------------------------------------------------------------------
# DAG and transformation configuration
# -------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    dag_id="transform_memberships_dag",
    default_args=default_args,
    description="Transform raw memberships data and flatten nested JSON",
    schedule_interval=None,
    start_date=datetime(2024, 10, 1),
    catchup=False,
)

# -------------------------------------------------------------------
# Core transformation function
# -------------------------------------------------------------------
def transform_memberships(**context):
    logging.info("🚀 Starting transformation for Memberships...")

    extracted_dir = "/home/ec2-user/airflow/extracted_data"
    transformed_dir = "/home/ec2-user/airflow/transformed_data"
    os.makedirs(transformed_dir, exist_ok=True)

    # Find latest extracted CSV
    extracted_files = [f for f in os.listdir(extracted_dir) if f.startswith("GetMemberships")]
    if not extracted_files:
        raise FileNotFoundError("❌ No extracted membership file found in extracted_data/")
    latest_file = sorted(extracted_files)[-1]
    file_path = os.path.join(extracted_dir, latest_file)
    logging.info(f"📂 Using extracted file: {file_path}")

    # Extract client_id from filename (if available)
    try:
        parts = latest_file.split("-")
        client_id = int(parts[1])
    except Exception:
        client_id = None
    logging.info(f"🔢 Extracted client_id from filename: {client_id}")

    # Load raw CSV
    df = pd.read_csv(file_path)
    logging.info(f"📊 Loaded {len(df)} rows")

    records = []

    # Flatten membership data
    for _, row in df.iterrows():
        club_type = row.get("clubName")
        club_id = row.get("id")
        members_str = row.get("members")

        if pd.isna(members_str) or not str(members_str).strip():
            continue

        try:
            members_list = ast.literal_eval(members_str)
        except Exception as e:
            logging.warning(f"⚠️ Failed to parse members for club_id={club_id}: {e}")
            continue

        for member in members_list:
            cust = member.get("customerInfo", {})
            records.append({
                "club_type": club_type,
                "id": member.get("id"),
                "signup_date": member.get("signUpDate"),
                "cancel_date": member.get("cancelDate"),
                "client_id": client_id,
                "customer_number": cust.get("customerNumber"),
                "email": cust.get("email"),
                "first_name": cust.get("firstName"),
                "last_name": cust.get("lastName"),
                "address1": cust.get("address1"),
                "city": cust.get("city"),
                "state": cust.get("state"),
                "zip": cust.get("zip")
            })

    # Convert to DataFrame
    df_clean = pd.DataFrame(records)
    logging.info(f"🧾 Flattened {len(df_clean)} membership records")

    # Clean up timestamp columns
    df_clean["signup_date"] = pd.to_datetime(df_clean["signup_date"], errors="coerce")
    df_clean["cancel_date"] = pd.to_datetime(df_clean["cancel_date"], errors="coerce")

    # Replace NaN / NaT with None
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    # Drop records missing both customer_number and email (invalid entries)
    df_clean.dropna(subset=["customer_number", "email"], how="all", inplace=True)
    logging.info(f"✅ Cleaned DataFrame shape: {df_clean.shape}")

    # Save cleaned data
    output_file = os.path.join(
        transformed_dir,
        f"TransformedMemberships-{datetime.now().strftime('%Y-%m-%d')}.csv"
    )
    df_clean.to_csv(output_file, index=False)
    logging.info(f"🎯 Transformed data saved to: {output_file}")
    logging.info("🎉 Transformation complete (no DB load).")


# -------------------------------------------------------------------
# Airflow task
# -------------------------------------------------------------------
transform_task = PythonOperator(
    task_id="transform_memberships",
    python_callable=transform_memberships,
    dag=dag,
)

# DAG entrypoint
transform_task
