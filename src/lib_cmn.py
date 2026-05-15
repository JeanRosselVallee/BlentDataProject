import os
import logging
from pathlib import Path

def setup_logging() -> None:
    """Setup logging from environment variables."""

    level_str = os.getenv("ETL_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)

    log_file = os.getenv("ETL_LOG_FILE")  # e.g. "logs/etl.log"
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        p = Path(log_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        handlers.insert(0, logging.FileHandler(p, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )