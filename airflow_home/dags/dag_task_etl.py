"""
# Airflow Orchestration DAG
## 🚀 Pipeline: Daily Scan ETL

For more details cf. scripts/run_etl.py
### 📋 Overview
This DAG automates the daily extraction, validation, and 
loading of scan logs into our Data Warehouse (DWH).
#### Architecture Components:
- **Web Server:** GUI to monitor task status & trigger manual runs.
- **Scheduler:** Monitoring engine, triggers workflows on schedule.
- **Database:** Metadata store for task states & DAG definitions.

### 🛠️ Operational Guide
If you need to manually interact with this pipeline via the terminal:

* **To start the background servers:** Run without parameters.
    ```bash
    ./airflow_run_etl.sh
* **To troubleshoot/test a single day:** Pass a single date partition.
    ```bash
    ./airflow_run_etl.sh 2026-05-19
* **To backfill missed data:** Pass a start and end range.
    ```bash
    ./airflow_run_etl.sh 2026-04-01 2026-04-10
    ```
### 🔐 Required Configuration (.env)
This pipeline dynamically adjusts boundaries based on your local 
configuration. Ensure your .env contains:

    AIRFLOW_DAG_START_DATE (e.g., 2026-03-01)

Owner: Jean Vallee| Last Updated: May 2026
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
TARGET_SCRIPT = PATH_PROJECT_ROOT / "scripts" / "run_etl.py"
ETL_VENV_PYTHON = PATH_PROJECT_ROOT / ".venv_etl" / "bin" / "python"


# Get Airflow's Start Date
# Start date is the minimal valid scan date for Catchup & Backfill
START_DATE = os.getenv("AIRFLOW_DAG_START_DATE", "2026-03-01")

try:  # Convert date from "YYYY-MM-DD" to Datetime
    parsed_start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
except ValueError:
    # If date in .env has a typo, use default value to avoid crash
    parsed_start_date = datetime(2026, 3, 1)

# Define DAG graph of tasks
with DAG(
    dag_id='daily_scan',
    start_date=datetime(2026, 3, 1),  # date parameter of airflow
    schedule='5 0 * * *',  # Scheduled daily at 0h5m UTC
    catchup=False,  # set to True for backfilling
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
