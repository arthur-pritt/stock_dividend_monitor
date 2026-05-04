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

def count_nyse_trading_days(start_date, end_date, inclusive=True, calendar_name= 'NYSE'):
    """
    Counts the number of valid NYSE trading dates and returns an integer."""

    #Step 0: Validate: if start_date is greater than end date, the system raises  valuerror.
    if start_date > end_date:
        raise ValueError(f"ERROR: Start_date ({start_date}) cannot be greater than ({end_date})")
    
    #Step 1: Get the NYSE calendar
    calendar= mcal.get_calendar(calendar_name)

    #Step 2: Get the schedule
    schedule= calendar.schedule(start_date,end_date)

    #Step 3: Normalize to date and convert everything to UTC.
    trading_dates=pd.to_datetime(schedule.index.date).tz_localize('UTC')
    trading_dates=trading_dates.drop_duplicates(keep='first')
    

    #Step 4: Count valid trading days
    valid_trading_days=len(trading_dates)

    #Step 5: Handle special days
    if start_date == end_date:
        if valid_trading_days == 1 and inclusive:
            return 1
        else:
            return 0
        
    #Step 6: Handle inclusive= False for different dates
    if not inclusive and valid_trading_days > 0:
        dates_str=trading_dates.strftime('%Y-%m-%d')
        if dates_str[0]==start_date:
            valid_trading_days -=1
        if valid_trading_days > 0 and dates_str[-1]== end_date:
            valid_trading_days -= 1
            
    logger.info("COMPLETE: Count NYSE Trading Days Working Fine")
    return valid_trading_days

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
        validated_df=CURRENT_PRICE_FILE_SCHEMA.validate(df,lazy=True)
        logger.info(f"Pandera validation successfully passed")
    except pa.errors.SchemaErrors as err:
        logger.error(f"Pandera Validation Failed")
        logger.error(err.failure_cases)
        raise

    #Convert ticker to uppercase for use with yfinance
    validated_df[DATA_COLS['ticker']]=validated_df[DATA_COLS['ticker']].str.upper().str.strip()

    # Then drop NaNs in market_cap
    validated_df = validated_df.dropna(subset=["market_cap"])

    logger.info(f"Final validated dataset: {len(validated_df)} rows after dropping NaNs")

    return validated_df

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


    



