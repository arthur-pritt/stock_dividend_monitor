import os
import  pandas as pd
import logging
from typing import Optional

#importing datetime
from datetime import datetime
from config.logging_config import get_logger, setup_logging

#initialize logging system
setup_logging()

# Then get logger
logger = get_logger(__name__)

#importing config files
from config.settings import RAW_DATA_PATH

def load_nasdaq_data()-> Optional[pd.DataFrame]:
    """Loading nasdaq csv list from data folder
       
    Returns:
     Optional[pd.DataFrame]= Dataframe containing Nasdaq data if successful or
                                 None if the file is missing, empty, or unreadable.
    """

    #accessing the nasdaq csv path
    nasdaq_csv_path = RAW_DATA_PATH
    logger.info(f"Loading from {nasdaq_csv_path}")
    logger.info(f"File exists at {nasdaq_csv_path.exists()}")

    #loading the data
    try:
        df = pd.read_csv(nasdaq_csv_path)
        logger.info(f"SUCCESS: Data has been loaded {len(df)}")
        return df
    except FileNotFoundError:
        logger.error(f"Error: File not found at {nasdaq_csv_path}")
        logger.info(f" Ensure the file exists at data/raw/ folder")
        return None 
    except pd.errors.EmptyDataError:
        logger.error("Error: The csv file is empty")
        return None 
    except Exception as e:
        logger.error(f"Unexpected error loading data:{type(e).__name__}: {e}")
        return None 
#Usage with type hints 

if __name__ == "__main__":
    from config.logging_config import setup_logging

    #initialize logging system
    setup_logging()

    df: Optional[pd.DataFrame] = load_nasdaq_data()
    
    


