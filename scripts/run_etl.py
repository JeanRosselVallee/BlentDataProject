"""
Run the Phase 1 daily ETL job:
- Connect to MongoDB (Atlas)
- Fetch last 6 months of reviews
- Aggregate per game
- Select Top 15
- Upsert into PostgreSQL DWH

This script is intended to be executed by Render Cron Job.
Configuration is read from env variables ETL_* in etl_lib/config.py).
"""

import sys
import libraries.lib_etl  as lib_etl
import libraries.lib_cmn  as lib_cmn
import logging

from dotenv import load_dotenv
from libraries.config import load_env


def main() -> int:
    """Main function to run the ETL."""

    # Load environment variables
    load_dotenv()  # local dev 
    mongo_cfg, pg_cfg, etl_cfg = load_env()

    # Setup logging
    lib_cmn.setup_logging() # setup logging from environment variables

    # 1) Connect to DB's

    # Datalake DB & Collection
    mongo_client = lib_etl.connect_mongo(uri=mongo_cfg.uri)
    # Seed once if empty 
    lib_etl.seed_datalake(
        mongo_client=mongo_client,
        database=mongo_cfg.database,
        collection_name=mongo_cfg.collection,
        jsonl_path=etl_cfg.seed_file  
    )
    db_datalake = mongo_client[mongo_cfg.database]
    collection_datalake = db_datalake[mongo_cfg.collection]

    # DWH DB
    db_dwh = lib_etl.connect_postgres(dsn_string=pg_cfg.dsn)
    lib_etl.init_dwh(db_engine=db_dwh, table_name=pg_cfg.table_name)

    # Set Time Frame
    # timeframe_end = lib_etl.get_max_timestamp(collection_datalake)
    timeframe_end = lib_etl.utc_now()

    timeframe_start = lib_etl.get_timeframe_start(
        timeframe_end=timeframe_end, 
        lookback_months=etl_cfg.lookback_months
    )

    # Pipeline : Extract from Datalake & Transform
    df_top_products = lib_etl.extract_and_transform(
        reviews=collection_datalake,
        timeframe_start=timeframe_start,
        top_n=etl_cfg.top_n
    )

    if df_top_products.empty:
        logging.warning("⚠️ No Top products found. Skipping DWH load.")
    
    # Load to DWH
    lib_etl.upsert_dwh(
        db_dwh=db_dwh, 
        table_name=pg_cfg.table_name, 
        df=df_top_products
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
