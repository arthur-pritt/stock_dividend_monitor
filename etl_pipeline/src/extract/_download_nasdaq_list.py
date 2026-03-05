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
    #Boundaries IN-Validating/check the data/file path before doing anything

    #accessing the nasdaq csv path/storing the file path
    nasdaq_csv_path = RAW_DATA_PATH
    if nasdaq_csv_path is None:
        logger.error(f"No file path provided")
        return None 
    if not nasdaq_csv_path.exists():
        logger.error(f" File not found at {nasdaq_csv_path}")
        return None 
    if nasdaq_csv_path.suffix !='.csv':
        logger.error(f"Expected CSV, got {nasdaq_csv_path}")
        return None
    
    #=============PROCESSING=======

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
    except PermissionError:
        logger.error(f"Permission denied: Operating system blocked python to read. Cannot READ: {nasdaq_csv_path}")
        logger.error(f"Check if you have permission to read the file")
        
        return None
    except pd.errors.ParserError as e:
        logger.error(f"Pandas failed to read/format is broken.CSV parser error at {nasdaq_csv_path}")
        logger.error(f" {e}")
        logger.error(f"Check if the file is corrupted or invalid")
        return None 
    except pd.errors.EmptyDataError:
        logger.error("Error: The csv file is empty")
        return None 
    except Exception as e:
        logger.error(f"Unexpected error loading data:{type(e).__name__}: {e}")
        return None 
    
    #===========BOUNDARIES OUT=============
    
    if df is None or df.empty:
        logger.error(f"File loaded but contains no data")
        return None 
    

    required_columns = ['Symbol','Name','Market Cap']
    
    missing_columns = []

    for col in required_columns:
        if col not in df.columns:
            missing_columns.append(col)
    
    if missing_columns:
        logger.error(f"Missing expected columns:{missing_columns}")
        return None 
    
    if df['Symbol'].isnull().any():
        logger.warning("Some symbols have missing rows-FLAGGED FOR REVIEWS")
        return None

    

#Usage with type hints 

if __name__ == "__main__":
    from config.logging_config import setup_logging

    #initialize logging system
    setup_logging()

    df: Optional[pd.DataFrame] = load_nasdaq_data()

    logger.info(f"SUCCESS:{len(df)} Symbol loaded successfully")
    print(df.iloc[0:50])
    
    
    


