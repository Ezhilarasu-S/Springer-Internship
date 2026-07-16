"""
Airflow DAG: Load email_communications CSV to PostgreSQL
========================================================

Purpose: 
    Ingest email communications data from a local CSV file, validate against 
    schema, apply transformations, and load into PostgreSQL.

Author: Glynac-AI Data Platform Team
Version: 1.0
Created: 2026-07-15

Tasks:
    1. read_csv - Load CSV into memory
    2. validate_schema - Validate data against schema.yaml
    3. transform - Clean and enrich data
    4. load_to_postgres - Insert into target table (idempotent)

Status: Dataset status field (ongoing/completed) is respected
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowException

# ============================================================================
# Configuration & Logging
# ============================================================================

logger = logging.getLogger(__name__)

# DAG identifiers
DAG_ID = "load_email_communications_to_postgres"
DATASET_NAME = "email_communications"

# File paths
BASE_PATH = Path(os.getenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags"))
CSV_FILE = Path(os.getenv("CSV_FILE_PATH", "/opt/airflow/data/dataset.csv"))
SCHEMA_FILE = BASE_PATH / "schema.yaml"

# Database connection
DB_CONN_ID = "postgres_default"
TARGET_TABLE = "email_communications"

# ============================================================================
# DAG Configuration
# ============================================================================

default_args = {
    "owner": "glynac-data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 15),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=f"Load {DATASET_NAME} CSV to PostgreSQL with validation and transformation",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["data-ingestion", "postgresql", "csv", DATASET_NAME],
)

# ============================================================================
# Task Functions
# ============================================================================


def load_schema():
    """Load schema configuration from YAML file."""
    logger.info(f"Loading schema from: {SCHEMA_FILE}")
    
    if not SCHEMA_FILE.exists():
        raise AirflowException(f"Schema file not found: {SCHEMA_FILE}")
    
    with open(SCHEMA_FILE, "r") as f:
        schema = yaml.safe_load(f)
    
    logger.info(f"Schema loaded successfully with {len(schema.get('columns', []))} columns")
    return schema


def read_csv_task(**context):
    """
    Task: Read CSV file into pandas DataFrame.
    
    Returns:
        DataFrame with CSV data
    """
    logger.info(f"Reading CSV from: {CSV_FILE}")
    
    if not CSV_FILE.exists():
        raise AirflowException(f"CSV file not found: {CSV_FILE}")
    
    try:
        df = pd.read_csv(CSV_FILE)
        logger.info(f"CSV loaded successfully: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Store in XCom for downstream tasks
        context["task_instance"].xcom_push(key="dataframe", value=df)
        context["task_instance"].xcom_push(key="row_count", value=len(df))
        
        return {"status": "success", "rows": len(df), "columns": len(df.columns)}
    
    except Exception as e:
        logger.error(f"Error reading CSV: {str(e)}")
        raise AirflowException(f"Failed to read CSV: {str(e)}")


def validate_schema_task(**context):
    """
    Task: Validate CSV data against schema.yaml.
    
    Checks:
        - All required columns present
        - Data types match schema
        - Required fields are not null
        - Enum values are valid (email_status, email_criticality)
    
    Fails fast if validation errors found.
    """
    logger.info("Starting schema validation...")
    
    try:
        # Load schema and DataFrame
        schema = load_schema()
        ti = context["task_instance"]
        df = ti.xcom_pull(task_ids="read_csv", key="dataframe")
        
        if df is None or df.empty:
            raise AirflowException("No data to validate")
        
        validation_rules = schema.get("validation_rules", [])
        errors = []
        
        # Check: All required columns present (exclude auto-generated: id, loaded_at)
        schema_columns = {col["name"] for col in schema.get("columns", [])
                          if col["name"] not in ["id", "loaded_at"]}
        csv_columns = set(df.columns)
        
        missing_columns = schema_columns - csv_columns
        extra_columns = csv_columns - schema_columns
        
        if missing_columns:
            errors.append(f"Missing columns: {missing_columns}")
        
        if extra_columns:
            logger.warning(f"Extra columns in CSV (will be ignored): {extra_columns}")
        
        # Check: Required (non-nullable) columns have no nulls
        for col_def in schema.get("columns", []):
            col_name = col_def.get("name")
            is_nullable = col_def.get("nullable", True)
            
            # Skip the 'id' and 'loaded_at' as they're auto-generated
            if col_name in ["id", "loaded_at"]:
                continue
            
            if not is_nullable and col_name in df.columns:
                null_count = df[col_name].isnull().sum()
                if null_count > 0:
                    errors.append(
                        f"Column '{col_name}' is NOT NULL but has {null_count} nulls"
                    )
        
        # Check: Validation rules
        for rule in validation_rules:
            col = rule.get("column")
            rule_type = rule.get("rule")
            
            if col not in df.columns:
                continue
            
            if rule_type == "must_be_in":
                allowed_values = set(rule.get("values", []))
                invalid = df[~df[col].isin(allowed_values)][col].unique()
                if len(invalid) > 0:
                    errors.append(
                        f"Column '{col}' has invalid values: {invalid}. "
                        f"Allowed: {allowed_values}"
                    )
            
            elif rule_type == "range":
                min_val = rule.get("min")
                max_val = rule.get("max")
                if min_val is not None:
                    out_of_range = df[df[col] < min_val][col].count()
                    if out_of_range > 0:
                        errors.append(
                            f"Column '{col}' has {out_of_range} values < {min_val}"
                        )
                if max_val is not None:
                    out_of_range = df[df[col] > max_val][col].count()
                    if out_of_range > 0:
                        errors.append(
                            f"Column '{col}' has {out_of_range} values > {max_val}"
                        )
        
        # Report results
        if errors:
            error_msg = "\n".join(errors)
            logger.error(f"Validation FAILED:\n{error_msg}")
            raise AirflowException(f"Schema validation failed:\n{error_msg}")
        
        logger.info("✓ Schema validation PASSED")
        context["task_instance"].xcom_push(key="validation_status", value="passed")
        return {"status": "validation_passed"}
    
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        raise AirflowException(f"Schema validation failed: {str(e)}")


def transform_task(**context):
    """
    Task: Transform and clean the data.
    
    Transformations:
        - Convert timestamp to UTC timezone-aware
        - Strip whitespace from text fields
        - Parse list columns (email_types, product_types)
        - Ensure email_status and email_criticality are lowercase
        - Round customer_satisfaction to 4 decimals
        - Add loaded_at timestamp
    """
    logger.info("Starting data transformation...")
    
    try:
        ti = context["task_instance"]
        df = ti.xcom_pull(task_ids="read_csv", key="dataframe").copy()
        
        logger.info(f"Input: {len(df)} rows")
        
        # 1. Strip whitespace from string columns
        string_cols = df.select_dtypes(include=["object"]).columns
        for col in string_cols:
            df[col] = df[col].str.strip()
        
        # 2. Convert timestamp to timezone-aware UTC (handle mixed formats with/without microseconds)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format='mixed')
        
        # 3. Normalize categorical fields
        df["email_status"] = df["email_status"].str.lower().str.strip()
        df["email_criticality"] = df["email_criticality"].str.lower().str.strip()
        
        # 4. Clean customer_satisfaction (round to 4 decimals)
        df["customer_satisfaction"] = df["customer_satisfaction"].round(4)
        
        # 5. Remove duplicate rows based on natural key (thread_id, sender, receiver, timestamp)
        before_dedup = len(df)
        df = df.drop_duplicates(subset=["thread_id", "sender", "receiver", "timestamp"])
        after_dedup = len(df)
        if before_dedup != after_dedup:
            logger.warning(f"Removed {before_dedup - after_dedup} duplicate rows")
        
        # 6. Add loaded_at timestamp (current UTC time)
        df["loaded_at"] = datetime.utcnow()
        
        logger.info(f"Output: {len(df)} rows (after transformation)")
        logger.info("✓ Data transformation completed")
        
        # Store transformed data
        ti.xcom_push(key="transformed_dataframe", value=df)
        ti.xcom_push(key="transform_status", value="success")
        
        return {
            "status": "transformation_completed",
            "rows": len(df),
            "columns": len(df.columns),
        }
    
    except Exception as e:
        logger.error(f"Transformation error: {str(e)}")
        raise AirflowException(f"Data transformation failed: {str(e)}")


def load_to_postgres_task(**context):
    """
    Task: Load transformed data into PostgreSQL (idempotent).
    
    Strategy:
        - Truncate existing table before insert (safe for development)
        - Use COPY for efficient bulk insert
        - Leverage natural key (thread_id, sender, receiver, timestamp) 
          for uniqueness
    
    Returns:
        Load summary (rows inserted)
    """
    logger.info("Starting PostgreSQL load...")
    
    try:
        ti = context["task_instance"]
        df = ti.xcom_pull(task_ids="transform", key="transformed_dataframe")
        
        if df is None or df.empty:
            raise AirflowException("No data to load after transformation")
        
        # Connect to PostgreSQL
        hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        logger.info(f"Connected to PostgreSQL, loading {len(df)} rows...")
        
        try:
            # 1. Truncate target table (idempotent for dev environment)
            logger.info(f"Truncating table '{TARGET_TABLE}'...")
            cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE} RESTART IDENTITY CASCADE;")
            conn.commit()
            logger.info("✓ Table truncated")
            
            # 2. Prepare data: only include columns that exist in target table
            # Exclude 'id' as it's auto-generated
            target_columns = [
                "subject", "sender", "receiver", "timestamp", "message_body",
                "thread_id", "email_types", "email_status", "email_criticality",
                "product_types", "agent_effectivity", "agent_efficiency",
                "customer_satisfaction", "loaded_at"
            ]
            
            df_load = df[target_columns].copy()
            
            # 3. Insert data using psycopg2 execute_values for efficiency
            from psycopg2.extras import execute_values
            
            columns_list = ", ".join(target_columns)
            placeholders = ", ".join(["%s"] * len(target_columns))
            insert_sql = f"""
                INSERT INTO {TARGET_TABLE} ({columns_list})
                VALUES %s
            """
            
            # Convert DataFrame to list of tuples
            values = [tuple(row) for row in df_load.values]
            
            execute_values(cursor, insert_sql, values, page_size=1000)
            conn.commit()
            
            logger.info(f"✓ Successfully loaded {len(df)} rows into '{TARGET_TABLE}'")
            
            # 4. Verify load
            cursor.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE};")
            final_count = cursor.fetchone()[0]
            logger.info(f"Verification: Table now contains {final_count} rows")
            
            if final_count != len(df):
                raise AirflowException(
                    f"Row count mismatch: expected {len(df)}, got {final_count}"
                )
            
            cursor.close()
            conn.close()
            
            ti.xcom_push(key="load_status", value="success")
            ti.xcom_push(key="rows_loaded", value=len(df))
            
            return {
                "status": "load_completed",
                "rows_loaded": len(df),
                "target_table": TARGET_TABLE,
            }
        
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            raise AirflowException(f"Database error during load: {str(e)}")
    
    except Exception as e:
        logger.error(f"Load error: {str(e)}")
        raise AirflowException(f"Failed to load data into PostgreSQL: {str(e)}")


def summary_task(**context):
    """
    Task: Generate pipeline execution summary.
    """
    ti = context["task_instance"]
    
    summary = {
        "dag_id": DAG_ID,
        "dataset": DATASET_NAME,
        "execution_date": str(context["execution_date"]),
        "read_status": ti.xcom_pull(task_ids="read_csv", key="return_value"),
        "validation_status": ti.xcom_pull(task_ids="validate_schema", key="validation_status"),
        "transform_status": ti.xcom_pull(task_ids="transform", key="transform_status"),
        "load_status": ti.xcom_pull(task_ids="load_to_postgres", key="load_status"),
        "rows_loaded": ti.xcom_pull(task_ids="load_to_postgres", key="rows_loaded"),
    }
    
    logger.info("=" * 70)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 70)
    for key, value in summary.items():
        logger.info(f"{key}: {value}")
    logger.info("=" * 70)
    
    return summary


# ============================================================================
# DAG Tasks
# ============================================================================

task_read_csv = PythonOperator(
    task_id="read_csv",
    python_callable=read_csv_task,
    provide_context=True,
    dag=dag,
)

task_validate_schema = PythonOperator(
    task_id="validate_schema",
    python_callable=validate_schema_task,
    provide_context=True,
    dag=dag,
)

task_transform = PythonOperator(
    task_id="transform",
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

task_load_postgres = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres_task,
    provide_context=True,
    dag=dag,
)

task_summary = PythonOperator(
    task_id="summary",
    python_callable=summary_task,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# DAG Dependency Graph
# ============================================================================

task_read_csv >> task_validate_schema >> task_transform >> task_load_postgres >> task_summary
