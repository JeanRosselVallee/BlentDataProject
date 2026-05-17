## Blent Data Project

Daily ETL that reads reviews from MongoDB (Atlas), computes the **Top 15** best-rated games using only the **last 6 months** of reviews, and upserts the daily snapshot into a PostgreSQL Data Warehouse (Render).

### Repository layout

- `src/libraries/`: reusable functions
- `scripts/run_etl.py`: runnable entrypoint (Render Cron Job target)
- `doc/`: project specification

### Environment variables

Set these (Render "Environment" settings or a local `.env` file):

- `ETL_MONGO_URI`
- `ETL_MONGO_DB`
- `ETL_MONGO_COLLECTION`
- `ETL_POSTGRES_DSN`
- Optional:
  - `ETL_LOOKBACK_MONTHS` (default `6`)
  - `ETL_TOP_N` (default `15`)

### Run locally (example)

Create a virtual env, install, then run:

```bash
python -m pip install -r requirements_etl.txt
python -m pip install -e .
python -m scripts.run_etl
```

which gets records of previous day.
For any other date, run this:

```bash
python scripts/run_etl.py --scandate="YYYY-MM-DD"
```

## Airflow Orchestration

This project uses Apache Airflow to manage the ETL lifecycle. The `run_etl.py` script is designed to be idempotent, allowing it to be executed in three different operational modes:

### 1. Daily Scheduled Run (Production)
The "Set-and-Forget" mode. Airflow's scheduler triggers the script automatically to process the most recently completed period.
* **Frequency:** Daily (at 00:05 UTC).
* **Date Logic:** Uses the `{{ ds }}` macro to pass the "logical date" (yesterday) to the `--scan_date` parameter.
* **Purpose:** Continuous data ingestion.

### 2. Backfill (Historical Recovery)
Used to process historical data or recover from extended system outages.
* **Mechanism:** Triggered via CLI using `airflow dags backfill` or by setting `catchup=True` with a past `start_date`.
* **Behavior:** Airflow generates a sequence of task instances, executing the script once for every day in the missing range.
* **Purpose:** Initial data loading or repairing large gaps in history.

### 3. Ad-Hoc / Manual Run (Maintenance)
Manual execution for a specific, isolated date.
* **Mechanism:** Triggered via the Airflow Web UI ("Trigger DAG w/ config") or directly from the terminal.
* **Date Logic:** A custom date is passed manually to the `--scan_date` argument.
* **Purpose:** Debugging, testing new logic, or re-running a specific day that failed due to external API/source issues.

---
**Note:** All modes call the same core logic:
`python scripts.run --scan_date <YYYY-MM-DD> --platform <PLATFORM>`
