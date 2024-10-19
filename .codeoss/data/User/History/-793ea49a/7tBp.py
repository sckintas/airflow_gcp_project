from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging
from datetime import timedelta
from io import StringIO
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/gcs/data/access_account.json'

# Default arguments for the DAG TEST
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Table details
TABLES = ['EXPANDED_ASSEMBLY_PROCESS', 'EXPANDED_CORE_PREPARATION', 'EXPANDED_FLASK_PREPARATION', 'EXPANDED_LADLE_PREPARATION', 'EXPANDED_POURING_PROCESS','EXPANDED_TRANSPORT_TRACKING']
DATABASE = 'SNOW_GCP'
SCHEMA = 'raw'

# GCS and BigQuery details
BUCKET_NAME = 'snow_gcp001'
PROJECT_ID = 'mystical-sweep-439114-m6'
DATASET_NAME = 'snow_gcp'

# Table schemas for BigQuery
TABLE_SCHEMAS = {
    'EXPANDED_ASSEMBLY_PROCESS': [
        {"name": "ASSEMBLY_ID", "type": "STRING"},
        {"name": "FLASK_ID", "type": "STRING"},
        {"name": "CORE_ID", "type": "STRING"},
        {"name": "STAGE", "type": "NUMERIC"},
        {"name": "PROCESS", "type": "STRING"},
        {"name": "ARRIVAL_TIME", "type": "TIMESTAMP"},
        {"name": "DEPARTURE_TIME", "type": "TIMESTAMP"},
        {"name": "ESTIMATED_TIME", "type": "NUMERIC"}
    ],
    'EXPANDED_CORE_PREPARATION': [
        {"name": "CORE_ID", "type": "STRING"},
        {"name": "STAGE", "type": "NUMERIC"},
        {"name": "PROCESS", "type": "STRING"},
        {"name": "CHEMICALS_USED", "type": "STRING"},
        {"name": "ARRIVAL_TIME", "type": "TIMESTAMP"},
        {"name": "DEPARTURE_TIME", "type": "TIMESTAMP"},
        {"name": "ESTIMATED_TIME", "type": "NUMERIC"}
    ],
    'EXPANDED_FLASK_PREPARATION': [
        {"name": "FLASK_ID", "type": "STRING"},
        {"name": "STAGE", "type": "NUMERIC"},
        {"name": "PROCESS", "type": "STRING"},
        {"name": "CHEMICALS_USED", "type": "STRING"},
        {"name": "ARRIVAL_TIME", "type": "TIMESTAMP"},
        {"name": "DEPARTURE_TIME", "type": "TIMESTAMP"},
        {"name": "ESTIMATED_TIME", "type": "NUMERIC"}
    ],
    'EXPANDED_LADLE_PREPARATION': [
        {"name": "LADLE_ID", "type": "STRING"},
        {"name": "MELT_ID", "type": "STRING"},
        {"name": "STAGE", "type": "NUMERIC"},
        {"name": "PROCESS", "type": "STRING"},
        {"name": "CHEMICALS_USED", "type": "STRING"},
        {"name": "ARRIVAL_TIME", "type": "TIMESTAMP"},
        {"name": "DEPARTURE_TIME", "type": "TIMESTAMP"},
        {"name": "ESTIMATED_TIME", "type": "NUMERIC"}
    ],
    'EXPANDED_POURING_PROCESS': [
        {"name": "POURING_ID", "type": "STRING"},
        {"name": "ASSEMBLY_ID", "type": "STRING"},
        {"name": "LADLE_ID", "type": "STRING"},
        {"name": "MELT_ID", "type": "STRING"},
        {"name": "PROCESS", "type": "STRING"},
        {"name": "ARRIVAL_TIME", "type": "TIMESTAMP"},
        {"name": "DEPARTURE_TIME", "type": "TIMESTAMP"},
        {"name": "ESTIMATED_TIME", "type": "NUMERIC"}
    ],
    'EXPANDED_TRANSPORT_TRACKING': [
        {"name": "EQUIPMENT_ID", "type": "STRING"},
        {"name": "EQUIPMENT_TYPE", "type": "STRING"},
        {"name": "SOURCE_STAGE", "type": "NUMERIC"},
        {"name": "DESTINATION_STAGE", "type": "NUMERIC"},
        {"name": "TRANSPORT_MECHANISM", "type": "STRING"},
        {"name": "TRANSPORT_START_TIME", "type": "TIMESTAMP"},
        {"name": "TRANSPORT_END_TIME", "type": "TIMESTAMP"}
    ]
}

# DAG definition
with DAG(
    dag_id='snowflake_to_gcs_and_bigquery_with_dbt',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or set a schedule if needed
    catchup=False
) as dag:

    # Task 1: Extract from Snowflake and upload directly to GCS
    def extract_and_upload_to_gcs(**kwargs):
        """Extracts data from Snowflake and directly uploads to GCS."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake')
        gcs_hook = GCSHook(gcp_conn_id='gcp')

        for table_name in TABLES:
            query = f"SELECT * FROM {SCHEMA}.{table_name}"
            df = snowflake_hook.get_pandas_df(query)

            # Convert DataFrame to CSV string
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV string to GCS
            destination_blob = f'{table_name}.csv'
            gcs_hook.upload(
                bucket_name=BUCKET_NAME,
                object_name=destination_blob,
                data=csv_buffer.getvalue()
            )
            logging.info(f"Extracted {table_name} and uploaded to GCS: {destination_blob}")

    extract_and_upload_task = PythonOperator(
        task_id='extract_and_upload_to_gcs',
        python_callable=extract_and_upload_to_gcs,
    )

    # Task: Verify row count in Snowflake and GCS
    def verify_row_count_after_extract(**kwargs):
        """Verifies that the row counts match between Snowflake and the uploaded CSV in GCS."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake')
        gcs_hook = GCSHook(gcp_conn_id='gcp')

        success = True

        for table_name in TABLES:
            # Get row count from Snowflake
            snowflake_query = f"SELECT COUNT(*) AS count FROM {SCHEMA}.{table_name}"
            snowflake_count = snowflake_hook.get_first(snowflake_query)[0]

            # Read the CSV file from GCS
            csv_data = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=f'{table_name}.csv')
            csv_buffer = StringIO(csv_data.decode('utf-8'))
            csv_row_count = sum(1 for row in csv_buffer) - 1  # Subtract 1 for the header row

            logging.info(f"Snowflake count for {table_name}: {snowflake_count}")
            logging.info(f"GCS CSV row count for {table_name}: {csv_row_count}")

            # Compare the counts
            if snowflake_count != csv_row_count:
                success = False
                logging.error(f"Row count mismatch for {table_name}! Snowflake: {snowflake_count}, GCS: {csv_row_count}")

        if success:
            logging.info("All tables have matching row counts.")
        else:
            raise ValueError("Row count mismatch detected in one or more tables after extract.")

    # Python task to verify the row counts after extract
    verify_row_count_task = PythonOperator(
        task_id='verify_row_count_after_extract',
        python_callable=verify_row_count_after_extract,
    )

    # Task 2: Create tables in BigQuery
    def create_bigquery_tables(**kwargs):
        """Creates BigQuery tables for all tables."""
        for table_name, schema in TABLE_SCHEMAS.items():
            create_table_task = BigQueryCreateEmptyTableOperator(
                task_id=f'create_{table_name}_table',
                gcp_conn_id='gcp',
                project_id=PROJECT_ID,
                dataset_id=DATASET_NAME,
                table_id=table_name,
                schema_fields=schema,
                exists_ok=True
            )
            create_table_task.execute(kwargs)
            logging.info(f"Created BigQuery table for {table_name}")

    create_tables_task = PythonOperator(
        task_id='create_bigquery_tables',
        python_callable=create_bigquery_tables,
    )

    # Task 3: Load all CSVs from GCS into BigQuery
    def load_all_csvs_to_bigquery(**kwargs):
        """Loads all CSVs from GCS into BigQuery."""
        for table_name, schema in TABLE_SCHEMAS.items():
            source_uri = f'gs://{BUCKET_NAME}/{table_name}.csv'
            load_job = BigQueryInsertJobOperator(
                task_id=f'load_{table_name}_to_bigquery',
                gcp_conn_id='gcp',
                configuration={
                    "load": {
                        "sourceUris": [source_uri],
                        "destinationTable": {
                            "projectId": PROJECT_ID,
                            "datasetId": DATASET_NAME,
                            "tableId": table_name,
                        },
                        "sourceFormat": "CSV",
                        "writeDisposition": "WRITE_TRUNCATE",
                        "schema": {"fields": schema},
                        "skipLeadingRows": 1  # Skip the header row in the CSV
                    }
                }
            )
            load_job.execute(kwargs)
            logging.info(f"Loaded {table_name} from GCS into BigQuery")

    load_task = PythonOperator(
        task_id='load_all_csvs_to_bigquery',
        python_callable=load_all_csvs_to_bigquery,
    )

run_dbt = BashOperator(
    task_id='run_dbt_models',
    bash_command="""
    cd /home/airflow/gcs/data/dbt_project
    dbt run --profiles-dir /home/airflow/gcs/data/dbt_project/.dbt --target dev
    """,
    execution_timeout=timedelta(minutes=10)
)
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command="""
    cd /home/airflow/gcs/data/dbt_project
    dbt test --profiles-dir /home/airflow/gcs/data/dbt_project/.dbt --target dev
    """,
    execution_timeout=timedelta(minutes=10)
)

    # Set task dependencies: Extract & Upload -> Create BigQuery Tables -> Load into BigQuery -> Run dbt models
# Set task dependencies: Extract & Upload -> Verify Row Counts -> Create BigQuery Tables -> Load into BigQuery -> Run dbt models
extract_and_upload_task >> verify_row_count_task >> create_tables_task >> load_task >> run_dbt >> run_dbt_tests


