import pandas as pd
import  time
import pandera.pandas as pa
import pandas_market_calendars as mcal
import random 
from itertools import islice

from etl_pipeline.src.schema.ticker_schemas import TICKER_SCHEMA,CURRENT_PRICE_FILE_SCHEMA

from config.logging_config import get_logger
from config.settings import DATA_COLS

logger= get_logger(__name__)

def validate_tickers(df):
    #Basic validation
    if df is None:
        raise ValueError(f"The input dataframe is None. Provide a valid dataframe")
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a pandas dataframe. Got {type(df).__name__}")
    if df.empty:
        raise ValueError(f"The dataframe is empty.")
    
    #Row count validation
    n_rows=len(df)
    if n_rows <110:
        raise ValueError(f"DataFrame has only {n_rows} rows. Minimum required is 110 rows. "
            f"Received {n_rows} rows.")
    elif 110 <= n_rows <=249:
        logger.warning(f"DataFrame has {n_rows} rows. This is below the ideal range of 250-300. "
            f"Pipeline will continue but results may be limited.")
    else:
        pass
    
    #Pandera validation
    try:
        validate_df=CURRENT_PRICE_FILE_SCHEMA.validate(df,lazy=True)
        logger.info(f"Pandera validation successfully passed")
    except pa.errors.SchemaErrors as err:
        logger.error(f"Pandera Validation Failed")
        logger.error(err.failure_cases)
        raise

    #Drops rows where valuation is NaN
    validate_df=validate_df.dropna(subset=[DATA_COLS['valuations']])
    #Convert ticker to uppercase for use with yfinance
    validate_df[DATA_COLS['ticker']].str.upper().str.strip()
    return validate_df

if __name__ == "__main__":
    from config.logging_config import setup_logging
    from etl_pipeline.src.extract._download_nasdaq_list import load_nasdaq_data
    from etl_pipeline.src.extract._clean_nasdaq_data import (
        validate_top_300,validate_top_300,
        extract_columns,validateInData,normalize_names,build_master_list,match_and_categorize,
        get_top_300)
    setup_logging()
    logger.info("Starting to Fetch the Current Closing DAY prices=========")

    #Extract + Prep
    # Extract + prep
    df = load_nasdaq_data()
    df = validateInData(df)
    df = extract_columns(df)
    df = normalize_names(df)

    master = build_master_list(df)
    df = match_and_categorize(df, master)

    top_300 = get_top_300(df)
    top_300 = validate_top_300(top_300)

    #Fetch + Process
    tickers= validate_top_300(top_300)
    tickers=validate_tickers(tickers)
    print(tickers.info)


    



