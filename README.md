## Blent Data Project

### Phase 1 goal (implemented first)

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
python -m pip install -r requirements.txt
python -m pip install -e .
python scripts/run_etl.py
```

Note: the ETL functions are stubs right now (`NotImplementedError`). Next step is to implement them against your Atlas schema and your PostgreSQL table.