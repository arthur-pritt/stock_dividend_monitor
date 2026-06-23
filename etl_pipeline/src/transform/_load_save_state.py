import json 
from datetime import datetime, timezone, timedelta
from pathlib import Path 

from config.logging_config import (
    setup_logging,
    get_logger)
 
from config.settings import(
    DAILY_PRICE_FILEPATH,
    RAW_SUBDIR
)
setup_logging()
logger = get_logger()

RAW_SUBDIR.mkdir(parents=True, exist_ok=True)

METADATA_FILEPATH = DAILY_PRICE_FILEPATH.with_suffix('.json')

def load_pipeline_state():
    """Loads the current pipeline metadata state from disk."""

    if not METADATA_FILEPATH.is_file():
        return {}
    try:
        with open(METADATA_FILEPATH,'r', encoding='utf-8') as f:
            return json.load(f)
    
    except json.JSONDecodeError:
        logger.error(f"Metadata file {METADATA_FILEPATH} is corrupted.Resetting state. ")
        return {}
    
def save_pipeline_state(status: str, nan_count:int, success_count:int):
    """Generates and writes the standard UTC metadata payload to disk."""
    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    state = {
        "last_fetch_attempt": now_str,
        "last_fetch_status": status,
        "nan_ticker_count": nan_count,
        "successful_ticker_count":success_count
    }

    #Only add retry_after if the run wasn't fully successful
    if status in ["partial_nan", "failed"]:
        retry_time = now_utc + timedelta(hours=4)
        state["retry_after"] = retry_time.strftime("%Y-%m-%d %H:%M:%S UTC")

    with open(METADATA_FILEPATH, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)
    logger.info(f"Pipeline state updated to: {status.upper()}")

