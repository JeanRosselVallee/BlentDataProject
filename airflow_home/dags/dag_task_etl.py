"""
### Airflow Orchestration DAG
Goal: Orchestrate the daily execution of a target script.

Syntax (CLI):

- Use Case 1 : Run task on a given date 
    - Date value is passed to the task's Bash command's argument "scan_date"
    - 
    - Example:
        airflow tasks test daily_scan task_run_etl 2026-05-17

- Use Case 2 : "Backfill" = run task on a series of dates 
    - Airflow loops on all dates from "--start-date" to "--end-date"
    - Example:
        airflow dags backfill daily_scan --start-date 2024-05-01

Architecture Components:
- Web Server: GUI for monitoring task status and triggering manual runs.
- Scheduler: Monitoring engine that triggers workflows based on schedules.
- Database: Metadata store for task states & DAG definitions.

For more details on the ETL Pipeline cf. scripts/run_etl.py
"""

import os

from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


# Get Path of PROJECT_ROOT
PROJECT_ROOT = os.getenv(  # get from environment variables
    "PROJECT_ROOT",
    default=str(Path(__file__).resolve().parents[2])  # root is 2 levels up
)
PATH_PROJECT_ROOT = Path(PROJECT_ROOT)

# Set Paths to Target script & environment
TARGET_SCRIPT = PATH_PROJECT_ROOT / "scripts" / "run_etl.py"  # target script
ETL_VENV_PYTHON = PATH_PROJECT_ROOT / ".venv_etl" / "bin" / "python"  # target .venv

# Default configuration of  DAG
# default_args = {
#     'owner': 'blent_admin',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# Define DAG graph of tasks
with DAG(
    dag_id='daily_scan',
    start_date=datetime(2026, 5, 1),  # date parameter of airflow
    schedule='5 0 * * *',  # Scheduled daily at 0h5m UTC
    catchup=False,  # set to True for backfilling
    # tags=['production', 'etl', 'gaming'],  # UI search keywords in DAG catalog
    # default_args=default_args,
    # description='Daily scan - Get Top 15 products',
) as dag:
    dag.doc_md = __doc__  # documents' generation

    # Define Task to run script
    task_run_etl = BashOperator(
        task_id='task_run_etl',
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{ETL_VENV_PYTHON} -m scripts.run_etl "
            "--scan_date {{ ds }} "  # "ds" is provided by launcher script
            "--platform Airflow "
        )
    )
