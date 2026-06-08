import pandas as pd
import random 
import time
import pandera.pandas as pa
import pandas_market_calendars as mcal
from itertools import islice
from datetime import date 
from calendar import monthrange 
from edgar import Company
from edgar import set_identity
import os 
from dotenv import load_dotenv 

from config.logging_config import get_logger
from etl_pipeline.src.schema.ticker_schemas import CURRENT_PRICE_FILE_SCHEMA
from config.settings import DATA_COLS

logger = get_logger(__name__)
load_dotenv()
set_identity(os.environ.get("EDGAR_IDENTITY"))

def valicate_incoming_tickers(df):
    """Validate the inputs of 300 tickers and confornm the data is OK."""

    #CHECK/CONFIRM : data is Not None

    if df is None:
        raise ValueError("The input provided is None.")
    
    #CHECK/CONFIRM datatype

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a pandas dataframe but got {type(df).__name__}")
    
    #CHECK/CONFIRM if the dataeframe is empty

    if df.empty:
        raise ValueError(f"The datafram is empty.")
    
    #CHECK/CONFIRM row counts

    n_rows = len(df)

    if n_rows < 110:
        raise ValueError(f" Dataframe has only {n_rows} rows. Minimum require.")
    
    elif 110 <= n_rows <=249:
        logger.warning(f" Dataframe has {n_rows} rows. This is below ideal range of 250-300.")
        f"Received {n_rows} rows."

    else:
        pass 

    #Pandera validation

    try:
        validate_df= CURRENT_PRICE_FILE_SCHEMA.validate(df, lazy=True)
        logger.info(f"Pandera validation successfully passed.")

    except pa.errors.SchemaError as err:
        logger.error(f" pandera validation failed")
        logger.error(err.failure_cases)
        raise

    #Convert ticker to uppercase for consistency purpose

    validate_df[DATA_COLS['ticker']]= validate_df[DATA_COLS['ticker']].str.upper().str.strip()

    #Drop Nans in Market_cap
    validate_df = validate_df.dropna(subset=['market_cap'])
    logger.info(f"Final validated dataset: {len(validate_df)} rows after dropping Nans")
    return validate_df

def get_current_quarter(last_quarter=None):
    """
    A function that takes today's date, determines which quarter it falls into, and returns the quarter and the year.
    What quarter does today fall into? If current quarter is too early, use the reference point. If current quarter is ready, return it for fetching."""

    #Getting the current date, year, and month
    current_date= date.today()
    quarter = (current_date.month - 1)//3 + 1
    year = current_date.year 

    current_quarter=quarter
    current_year=year 

    #Conversions
    end_month = current_quarter *3
    _, last_day=monthrange(year, end_month)
    start_month=(current_quarter*3)-2
    start_day= 1
    start_date= date(year, start_month, start_day)
    end_date= date(year, end_month, last_day)
    quarter_year=[start_date, end_date]

    #Determining the quarter
    if last_quarter is None:
        return quarter_year 
    
    if current_year < last_quarter[1]:
        raise ValueError(f" This is anomaly. Current year can't be less than the last year")
    
    if current_year > last_quarter[1]:
        return quarter_year 
    
    if current_year == last_quarter[1]:
        if last_quarter[0] > current_quarter:
            raise ValueError(f" This is anomaly. Last quarter can't be greater  than the current quarter.")
        elif current_quarter > last_quarter[0]:
            #If fillings are not yet available, wait for 14 days.
            wait_time= current_date-start_date 
            wait_time= wait_time.days 
            the_previous_quarter=last_quarter[0]
            the_same_year= last_quarter[1]
            the_end_monthofthe_quarter=the_previous_quarter*3
            _, the_last_day0fthe_quarter=monthrange(the_same_year, the_end_monthofthe_quarter)
            the_start_monthofthe_quarter=(the_previous_quarter*3)-2
            the_start_dayofthe_quarter=1
            the_start_dateofthe_quarter= date(the_same_year, the_start_monthofthe_quarter, the_start_dayofthe_quarter)
            the_end_dateofthe_quarter=date(the_same_year, the_end_monthofthe_quarter,the_last_day0fthe_quarter)
            the_previous_quarter_period=[the_start_dateofthe_quarter, the_end_dateofthe_quarter,]

            if wait_time < 14:
                return the_previous_quarter_period
            else:
                return quarter_year 
            
        else:
            the_quarter= last_quarter[0]
            the_year=last_quarter[1]
            the_end_month=the_quarter*3
            _, the_last_day=monthrange(the_year, the_end_month)
            the_start_month=(the_quarter*3)-2
            the_start_day= 1
            the_start_date=date(the_year, the_start_month, the_start_day)
            the_end_date= date(the_year, the_end_month, the_last_day)

            previous_period=[the_start_date, the_end_date]

            return previous_period


if __name__ == "__main__":
    from config.logging_config import setup_logging
    from etl_pipeline.src.extract._download_nasdaq_list import load_nasdaq_data
    from etl_pipeline.src.extract._clean_nasdaq_list import(
        validate_top_300,
        extract_columns,
        validateInData,
        normalize_names,
        build_master_list,
        match_and_categorize,
        get_top_300
    )

    setup_logging()
    logger.info("Starting to Fetch earning per share data from SEC EDGAR API===")

    #Extract + Prep

    df = load_nasdaq_data()
    df = validateInData(df)
    df = extract_columns(df)
    df = normalize_names(df)
    master = build_master_list(df)
    df = match_and_categorize(df, master)

    top_300 = get_top_300(df)
    top_300 = validate_top_300(top_300)

    #Fetch earning per share prices + Process
    validate_tickers = valicate_incoming_tickers(top_300)
    date_range=get_current_quarter(last_quarter=[1,2026])
    print(date_range)
