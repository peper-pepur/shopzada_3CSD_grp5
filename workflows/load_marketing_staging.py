from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------

def get_source_path():
    base_paths = [
        '/opt/airflow/source/marketing-department',
        '/home/vgr/dev/school/shopzada_3CSD_grp5/source/marketing-department'
    ]
    
    for path in base_paths:
        if os.path.exists(path):
            print(f"Using source path: {path}")
            return path
    
    raise FileNotFoundError(f"Could not find source files in any of: {base_paths}")

def get_db_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'airflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/shopzada"
    )

def detect_and_load_file(file_path):
    file_ext = Path(file_path).suffix.lower()
    file_name = Path(file_path).name

    print(f"\nProcessing file: {file_name}")

    try:
        if file_ext == '.csv':
            try:
                df = pd.read_csv(file_path, index_col=0)
                df.reset_index(inplace=True)
                df.rename(columns={df.columns[0]: 'raw_index'}, inplace=True)
            except:
                df = pd.read_csv(file_path)
            print("  → Loaded as CSV")
        else:
            print(f"  ✗ Unsupported file type: {file_ext}")
            return None, None

        print(f"  → Shape: {df.shape}")
        return df, file_name

    except Exception as e:
        print(f"  ✗ Error loading file: {str(e)}")
        return None, None

# -------------------------------------------------------------------
# ETL Load Logic
# -------------------------------------------------------------------

def load_marketing_staging_data(**context):
    source_path = get_source_path()
    engine = get_db_engine()

    print("=" * 70)
    print("LOADING MARKETING DEPARTMENT DATA TO STAGING")
    print("=" * 70)

    all_files = list(Path(source_path).glob('*'))
    data_files = [f for f in all_files if f.is_file() and not f.name.startswith('.')]

    print(f"Found {len(data_files)} files to process:")
    for f in data_files:
        print(f"  - {f.name}")
    print()

    load_summary = []

    for file_path in data_files:
        df, file_name = detect_and_load_file(str(file_path))

        if df is None:
            continue

        # Decide target table
        if "campaign_data" in file_name.lower():
            table_name = "mkt_campaigns_raw"
            expected_cols = ['raw_index', 'campaign_id', 'campaign_name', 'campaign_description', 'discount']
        
        elif "transactional_campaign_data" in file_name.lower():
            table_name = "mkt_campaign_transactions_raw"
            expected_cols = ['raw_index', 'transaction_date', 'campaign_id', 'order_id',
                             'estimated_arrival', 'availed']
        
        else:
            print(f"⚠ Unrecognized file type for marketing: {file_name}")
            continue

        print(f" → Target table: staging.{table_name}")

        # Convert all fields to VARCHAR (staging requirement)
        for col in df.columns:
            df[col] = df[col].astype(str)

        # Add metadata
        df['_source_file'] = file_name
        df['_ingested_at'] = pd.Timestamp.now()

        # Select only expected columns + metadata
        available_cols = [c for c in expected_cols if c in df.columns]
        final_cols = available_cols + ['_source_file', '_ingested_at']

        df_to_load = df[final_cols].copy()

        print(f" → Loading {len(df_to_load)} rows with columns: {final_cols}")

        try:
            df_to_load.to_sql(
                name=table_name,
                con=engine,
                schema='staging',
                if_exists='append',
                index=False,
                method='multi'
            )

            print(f" ✓ Loaded into staging.{table_name}")
            load_summary.append({"file": file_name, "table": table_name, "rows": len(df_to_load), "status": "SUCCESS"})

        except Exception as e:
            print(f" ✗ Failed loading {file_name}: {str(e)}")
            load_summary.append({"file": file_name, "table": table_name, "rows": 0, "status": f"FAILED: {str(e)}"})

    print("=" * 70)
    print("LOAD SUMMARY")
    print("=" * 70)
    for item in load_summary:
        print(f"{item['file']} → {item['table']}: {item['status']} ({item['rows']} rows)")

    context['ti'].xcom_push(key='load_summary', value=load_summary)

# -------------------------------------------------------------------
# Truncate staging tables before load
# -------------------------------------------------------------------

def truncate_staging_tables():
    engine = get_db_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    print("Truncating marketing staging tables...")

    tables = [
        "staging.mkt_campaigns_raw",
        "staging.mkt_campaign_transactions_raw"
    ]

    for tbl in tables:
        try:
            cursor.execute(f"TRUNCATE TABLE {tbl};")
            print(f" ✓ Truncated {tbl}")
        except Exception as e:
            print(f" ⚠ Could not truncate {tbl}: {str(e)}")

    conn.commit()
    cursor.close()
    conn.close()

# -------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------

with DAG(
    'load_marketing_staging',
    default_args=default_args,
    description='Load marketing department data into staging schema',
    schedule_interval=None,
    catchup=False,
    tags=['staging', 'marketing', 'campaign'],
) as dag:

    truncate_task = PythonOperator(
        task_id='truncate_marketing_staging_tables',
        python_callable=truncate_staging_tables,
    )

    load_task = PythonOperator(
        task_id='load_marketing_staging_data',
        python_callable=load_marketing_staging_data,
        provide_context=True
    )

    truncate_task >> load_task
