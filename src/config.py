"""Configuration loading for the ETL phase (Phase 1).

Purpose:
- Centralize environment variables (MongoDB Atlas source + PostgreSQL DWH).
"""

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class MongoConfig:
    """MongoDB connection settings (Atlas or local)."""

    uri: str
    database: str
    collection: str


@dataclass(frozen=True)
class PostgresConfig:
    """PostgreSQL connection settings (Render-managed Postgres)."""

    dsn: str
    table_name: str


@dataclass(frozen=True)
class EtlConfig:
    """ETL runtime settings."""

    seed_file: str
    lookback_months: int = 6
    top_n: int = 15


def load_env() -> tuple[MongoConfig, PostgresConfig, EtlConfig]:
    """Load configuration from environment variables.

    Required:
    - MONGO_URI
    - MONGO_DB
    - MONGO_COLLECTION
    - POSTGRES_DSN
    - POSTGRES_TABLE_NAME

    Optional:
    - SEED_FILE (defaults to data/Video_Games_5.json)
    - LOOKBACK_MONTHS, TOP_N
    """

    mongo_uri = os.environ.get("MONGO_URI", "").strip()
    mongo_db = os.environ.get("MONGO_DB", "").strip()
    mongo_collection = os.environ.get("MONGO_COLLECTION", "").strip()
    pg_dsn = os.environ.get("POSTGRES_DSN", "").strip()
    pg_table_name = os.environ.get("POSTGRES_TABLE_NAME", "").strip()
    seed_file = os.environ.get("SEED_FILE", "data/Video_Games_5.json").strip()

    missing: list[str] = []
    if not mongo_uri:
        missing.append("MONGO_URI")
    if not mongo_db:
        missing.append("MONGO_DB")
    if not mongo_collection:
        missing.append("MONGO_COLLECTION")
    if not pg_dsn:
        missing.append("POSTGRES_DSN")
    if not pg_table_name:
        missing.append("POSTGRES_TABLE_NAME")
    if not seed_file:
        missing.append("SEED_FILE")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    mongo = MongoConfig(uri=mongo_uri, database=mongo_db, collection=mongo_collection)
    pg = PostgresConfig(dsn=pg_dsn, table_name=pg_table_name)
    try:
        etl = EtlConfig(
            seed_file=seed_file,
            lookback_months=int(os.environ.get("LOOKBACK_MONTHS", "6")),
            top_n=int(os.environ.get("TOP_N", "15")),
        )
    except ValueError as e:
        raise RuntimeError(f"Invalid integer value for configuration: {e}") from e
    
    return mongo, pg, etl
