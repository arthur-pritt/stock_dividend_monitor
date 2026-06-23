import json 
from datetime import datetime, timezone, timedelta
from pathlib import Path 

from config.logging_config import (
    setup_logging,
    get_logger)
 
setup_logging()
logger = get_logger()




def is_cooling_off(state:dict):
    """
    Verifies if the pipeline is currntly with a mandaory retry cool-off window"""

    retry_after_str = state.get("retry_after")
    if not retry_after_str:
        return False 
    
    try:
        #Strip the 'UTC' string suffix to parse safely
        clean_ts = retry_after_str.replace(" UTC", "")
        retry_after_dt = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

        if datetime.now(timezone.utc) < retry_after_dt:
            logger.warning(f"Pipeline short circuited. Cool-off active until {retry_after_str}")
            return True
        
    except Exception as e:
        logger.error(f" Failed to parse retry_after timestamp: {e}. Ignoring cool-off")

    return False 