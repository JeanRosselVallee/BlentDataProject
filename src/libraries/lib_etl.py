"""Core data pipeline functions (phase-oriented).

Purpose:
- Provide reusable building blocks for data ingestion / transformation / loading.
- Phase 1 implements a daily ETL from MongoDB (Atlas) to PostgreSQL (Render).
- Future phases can add new modules while reusing shared helpers from this package.
"""

import logging
import json
import psycopg # PostgreSQL connection
import pandas as pd

#from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Iterable
from dateutil.relativedelta import relativedelta
from pathlib import Path
from typing import Any
from sqlalchemy import text, Engine, create_engine  # Pandas <> Postgres

# @dataclass(frozen=True) # ~ constant instance
# class TopProduct:
#     """Daily snapshot record to store in DWH."""

#     product_id: str  
#     nb_reviews: int
#     average_rating: float
#     oldest_rating: float
#     newest_rating: float


# -----------------------------
# DB connections
# -----------------------------

def connect_mongo(*, uri: str):
    """Create and return a MongoDB client."""

    from pymongo import MongoClient
    
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=10_000,  # fail fast if URI/network is wrong
        connectTimeoutMS=10_000,
        socketTimeoutMS=20_000,
        retryWrites=True,
    )

    # Check connection
    try:
        client.admin.command("ping")
        logging.info("✅ Connected to MongoDB")    
    except Exception as e:         
        logging.error("❌ Failed to connect to MongoDB")
        logging.error(f"🛑 {str(e)}")
        raise e   

    return client


def connect_postgres(*, dsn_string: str):
    """Create and return a PostgreSQL connection."""
    try:    
        # Create DB Engine
        db_dwh = create_engine(dsn_string)
        logging.info("✅ Connected to DWH's Postgres DB")

        # Check DB access
        with db_dwh.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            access_status = (result.fetchone()[0] == 1)
            logging.info(f"✅ DWH DB accessed: {access_status}")

        return db_dwh

    except Exception as e:
        logging.error("❌ Failed to connect to DWH Postgres DB")
        logging.error(f"🛑 {str(e)}")
        raise e   


# -----------------------------
# Aggregation
# -----------------------------

def get_timeframe_start(
    *, 
    timeframe_end: int, 
    lookback_months: int
) -> int:
    """Compute the datetime threshold for the lookback window.

    We only include reviews newer than (timeframe_end - lookback_months).
    """
    if lookback_months <= 0:
        lookback_months = -lookback_months

    delta_datetime = relativedelta(months=lookback_months)
    end_timestamp = timeframe_end
    end_datetime = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)
    start_datetime = end_datetime - delta_datetime
    logging.info(f"timeframe_start={start_datetime}")

    start_timestamp = start_datetime.timestamp()
    return start_timestamp


def extract_and_transform(
    *,
    reviews: Iterable[dict[str, Any]],
    timeframe_start: int,
) -> pd.DataFrame:
    """Execute pipeline:
    1 - Filter recent reviews
    2 - Sort by timestamp
    3 - Group by product id "asin"
    4 - Sort by rating & nb of reviews
    5 - Get Top-15

    Output per product as Pandas:
    - average rating 
    - number of ratings
    - oldest review rating in window
    - newest review rating
    """

    # Pipeline Definition
    pipeline = [        
        {   # Filter recent reviews
            "$match": {
                "unixReviewTime": {"$gte": timeframe_start},
                "verified": True
            }
        },
        {   # Sort by timestamp
            "$sort": {"unixReviewTime": 1}
        },
        {   # Group by product id "asin"
            "$group": {
                "_id": "$asin",                          
                "nb_reviews": {"$sum": 1},             
                "average_rating": {"$avg": "$overall"},  
                "oldest_rating": {"$first": "$overall"},  
                "newest_rating": {"$last": "$overall"},  
            }
        },
        {   # Sort by rating & nb of reviews
            "$sort": {
                "average_rating": -1,
                "nb_reviews": -1
            }
        },
        {   # Get Top-15
            "$limit": 15  
        }
    ]

    # Execute Aggregation Pipeline
    result_cursors = list(reviews.aggregate(pipeline))

    # Get DataFrame
    df = pd.DataFrame([p for p in result_cursors])

    # Log Top-15 products
    logging.info(f"Top-15 products in Datalake\n{df}")
    return df

# -----------------------------
# Load (insert/upsert into DWH)
# -----------------------------

def upsert_dwh(
    *,
    db_dwh: Any,
    rows: pd.DataFrame,
) -> None:
    """Insert the daily Top-N snapshot into PostgreSQL (upsert).

    Recommended table key to avoid duplicates:
    - UNIQUE(run_date, asin)

    TODO:
    - Create table in Postgres and implement:
        INSERT ... ON CONFLICT (run_date, asin)
        DO UPDATE SET avg_rating = EXCLUDED.avg_rating, ...
    - Use executemany/batch inserts.
    - Commit transaction.
    """

def upsert_dwh(*, db_dwh, table_name: str, df: pd.DataFrame):

    # Add Snapshot Date
    df['snapshot_date'] = pd.Timestamp.now().date()
    

## To DO :
## Create table before ; Add index to table


    # Write to DWH Table
    df.to_sql(
        name=table_name, 
        con=db_dwh, 
        if_exists='append', # keeps historical data
        index=False
    )
    logging.info(f"✅ Successfully loaded {len(df)} records to DWH.")

    return


def utc_now() -> datetime:
    """Return current UTC time as timezone-aware datetime."""

    return datetime.now(timezone.utc)


def get_max_timestamp(collection:Any):
    last_record = collection.find_one(
        sort=[("unixReviewTime", -1)], 
        projection={"unixReviewTime": 1}
    )
    max_timestamp = last_record["unixReviewTime"]
    max_datetime = datetime.fromtimestamp(max_timestamp, tz=timezone.utc)
    logging.info(f"max_datetime={max_datetime}")
    return max_timestamp


def seed_datalake(
    *,  # all args should be key=value
    mongo_client: Any,
    database: str,
    collection: str,
    jsonl_path: str | Path,
    batch_size: int = 2_000,
) -> int:
    """
    Seed a MongoDB collection from a JSON Lines file, but only if the collection is empty.

    - Expects JSONL: one JSON object per line.
    - Uses batching to avoid loading the full file into memory.

    Returns the number of inserted documents (0 if already seeded).
    """
    col: Collection = mongo_client[database][collection]

    # If there is already at least one document, we consider the collection seeded.
    if col.find_one(projection={"_id": 1}) is not None:
        return 0

    # Check data file
    json_rel_path = Path(jsonl_path)  # jsonl_path is a string
    root_abs_path = Path(__file__).resolve().parents[2]  # root is 2 levels up
    json_abs_path = root_abs_path / json_rel_path
    if not json_abs_path.is_file():
        raise FileNotFoundError(f"Seed file not found: {json_abs_path}")

    inserted_total = 0
    batch: list[dict] = []

    with json_abs_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Get json record
            record = json.loads(line)

            # Remove longest column. MongoDb's max = 500 Mb
            record.pop("reviewText", None)

            # Fills batch 
            batch.append(record)

            # Until batch is full
            batch_is_full = (len(batch) >= batch_size)
            if batch_is_full:
                results = col.insert_many(batch, ordered=True)  # stops at first error
                inserted_total += len(results.inserted_ids)
                batch.clear()

                # Display count every 10_000
                if inserted_total % 10000 == 0:
                    logging.info(f"Partial count: {inserted_total}")

    # Case last batch is smaller
    if batch:  
        results = col.insert_many(batch, ordered=True)
        inserted_total += len(results.inserted_ids)

    logging.info(f"✅ Inserted {inserted_total} records in Datalake")
    return inserted_total

def init_dwh(db_engine: Engine, table_name: str) -> None:
    try:
        # Create Table
        query = text(f"""
            CREATE TABLE IF NOT EXISTS public.{table_name} (
                product_id VARCHAR(50),
                nb_reviews INTEGER,
                average_rating NUMERIC(3, 2),
                newest_rating NUMERIC(3, 2),
                snapshot_date DATE,
                -- No duplicates on same day
                PRIMARY KEY (product_id, snapshot_date) 
            );
            CREATE INDEX IF NOT EXISTS idx_snapshot_date
                ON public.daily_snapshot (snapshot_date);
        """)
        
        with db_engine.begin() as conn:
            conn.execute(query)
        logging.info(f"✅ Table {table_name} exists in DWH DB")
    except Exception as e:
        logging.error(f"❌ Table {table_name} absent in DWH DB")
        logging.error(f"🛑 {str(e)}")
        raise e   
