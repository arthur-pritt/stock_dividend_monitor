import pandas as pd
import  time
import pandas_market_calendars as mcal
import random 
from itertools import islice

from etl_pipeline.src.schema.ticker_schemas import TICKER_SCHEMA

from config.logging_config import get_logger
from config.settings import DATA_COLS

logger= get_logger(__name__)

def validate_tickers(df):
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a pandas dataframe. Got {type(df).__name__}")
    df.columns= [col.lower().replace(" ","_") for col in df.columns]
    return TICKER_SCHEMA.validate(df)

if __name__ == "__main__":
    from config.logging_config import setup_logging
    from etl_pipeline.src.extract._download_nasdaq_list import load_nasdaq_data
    setup_logging()
    logger.info("Starting to Fetch the Current Closing DAY prices=========")

    #Extract + Prep
    df= load_nasdaq_data()
    df=validate_tickers(df)

    #Fetch + Process
    tickers= validate_tickers(df)
    print(tickers.info)


    



