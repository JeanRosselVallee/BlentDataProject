"""
Run the Phase 1 daily ETL job:
- Connect to MongoDB (Atlas)
- Fetch last 6 months of reviews
- Aggregate per game
- Select Top 15
- Upsert into PostgreSQL DWH
This script is intended to be executed by orchestrator Airflow
Configuration is read from env variables ETL_* in etl_lib/config.py).
"""

import logging
import src.lib_etl as etl
import src.config as cfg

from datetime import datetime, timezone
from dotenv import load_dotenv


def main() -> int:
    """Main function to run the ETL."""

    # Get script's arguments: scan_date & platform
    args = etl.get_parsed_args()

    # Load environment variables
    load_dotenv()  # load environment variables in ./root_dir/.env
    mongo_cfg, pg_cfg, etl_cfg = cfg.load_env()

    # Setup logging
    etl.setup_logging() # setup logging from environment variables
    logging.info(f"🚀 ETL launched from platform: {args.platform}")

    # Connect to DB's
    try:
        # Datalake DB & Collection
        mongo_client = etl.connect_mongo(uri=mongo_cfg.uri)
        # Seed once if empty 
        etl.seed_datalake(
            mongo_client=mongo_client,
            database=mongo_cfg.database,
            collection_name=mongo_cfg.collection,
            jsonl_path=etl_cfg.seed_file  
        )
        db_datalake = mongo_client[mongo_cfg.database]
        collection_datalake = db_datalake[mongo_cfg.collection]

        # DWH DB
        db_dwh = etl.connect_postgres(dsn_string=pg_cfg.dsn)
        etl.init_dwh(db_engine=db_dwh, table_name=pg_cfg.table_name)

        # Set Time Frame
        if args.scan_date:
            try:
                scan_date = datetime.fromisoformat(args.scan_date).replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                logging.error(f"❌ Invalid format for scan_date: {args.scan_date}.")
                logging.error("Use YYYY-MM-DD.")
                return 1
        else:
            scan_date = etl.utc_now()
            # scan_date = etl.get_max_timestamp(collection_datalake)

        timeframe_start = etl.get_timeframe_start(
            scan_date=scan_date, 
            lookback_months=etl_cfg.lookback_months
        )

        # Pipeline : Extract from Datalake & Transform
        df_top_products = etl.extract_and_transform(
            reviews=collection_datalake,
            timeframe_start=timeframe_start,
            top_n=etl_cfg.top_n
        )

        if df_top_products.empty:
            logging.warning("⚠️ No Top products found. Skipping DWH load.")
        
        # Load to DWH
        etl.upsert_dwh(
            db_dwh=db_dwh, 
            table_name=pg_cfg.table_name, 
            df=df_top_products,
            snapshot_date=scan_date.date()
        )
        return 0

    except Exception as e:
        logging.error(f"❌ Main process interrupted: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
