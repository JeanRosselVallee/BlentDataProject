"""
### Airflow Orchestration DAG
Goal: Orchestrate the daily execution of the ETL pipeline.

Syntax (CLI):
- Test Task: 
    airflow tasks test daily_reviews_etl run_etl_script YYYY-MM-DD
- Backfill:  
    airflow dags backfill daily_reviews_etl --start-date YYYY-MM-DD

Architecture Components:
- Web Server: GUI for monitoring task status and triggering manual runs.
- Scheduler: Monitoring engine that triggers workflows based on schedules.
- Database: Metadata store for task states and DAG definitions (SQLite/Postgres).

For more details on the ETL Pipeline cf. scripts/run.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Default configuration of  DAG
default_args = {
    'owner': 'blent_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'daily_reviews_etl',
    default_args=default_args,
    description='Daily ETL to compute Top 15 best-rated products from MongoDB to Postgres',
    # Scheduled daily at 00:05 UTC
    schedule_interval='5 0 * * *',
    # Start date set to a fixed past date to allow for testing/backfilling
    start_date=datetime(2024, 5, 1),
    # Set catchup to False by default; set to True if you want to run historical dates automatically
    catchup=False,
    tags=['production', 'etl', 'gaming'],
) as dag:
    dag.doc_md = __doc__

    # Task to run the ETL script
    # We use the {{ ds }} macro which provides the logical date in YYYY-MM-DD format
    run_etl = BashOperator(
        task_id='run_etl_script',
        bash_command=(
            "cd d:/Dev/Blent_AI/BlentDataProject && "
            "python -m scripts.run "
            "--scan_date {{ ds }} "
            "--platform Airflow"
        ),
        # Ensure environment variables from the host/env are available
        append_env=True,
    )

    run_etl
