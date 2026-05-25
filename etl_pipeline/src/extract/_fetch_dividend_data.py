import pandas as pd
import random
import time
import pandera.pandas as pa
import pandas_market_calendars as mcal
from itertools import islice 
from datetime import date
from calendar import monthrange


from config.logging_config import get_logger
from etl_pipeline.src.schema.ticker_schemas import CURRENT_PRICE_FILE_SCHEMA
from config.settings import DATA_COLS

logger = get_logger(__name__)

def validate_dividend_tickers(df):
    """Validating the inputs of 300 tickers and comfirming the data is 
    OK."""

    #Check/confirm the data is not None.

    if df is None:
        raise ValueError("The input provided is None.")

    #Check/confirm datatype

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a pandas dataframe but got {type(df).__name__}")
    
    #Check/confirm if the dataframe is empty

    if df.empty:
        raise ValueError(f"The dataframe is empty.")
    
    #Check/confirm row counts

    n_rows= len(df)

    if n_rows < 110:
        raise ValueError(f" Dataframe has only {n_rows} rows. Minimum required is 110 rows")
           
    elif 110 <= n_rows <=249:
        logger.warning(f"Dataframe has {n_rows} rows. This is below the ideal range of 250-300.")
        f"Received {n_rows} rows."

    else:
        pass 

    #Pandera validation

    try:
        validate_df= CURRENT_PRICE_FILE_SCHEMA.validate(df,lazy=True)
        logger.info(f"Pandera Validation successfully passed.")
    except pa.errors.SchemarErrors as err:
        logger.error(f" Pandera validation Failed")
        logger.error(err.failure_cases)
        raise

    #Convert ticker to uppercase for consistency purposes

    validate_df[DATA_COLS['ticker']]= validate_df[DATA_COLS['ticker']].str.upper().str.strip()


    #Drop Nans in Market_cap
    validate_df= validate_df.dropna(subset=['market_cap'])

    logger.info(f"Final validated dataset: {len(validate_df)} rows after dropping Nans.")

    return validate_df

def get_current_quarter(last_quarter=None):
    """
    A function that takes today's date, determines which quarter it falls into, and returns the quarter and the year.
    What quarter does today fall into? If current quarter is too early, use the reference point. If current quarter is ready, return it for fetching.
    """

    #Getting the current date, year, and month

    current_date = date.today()
    quarter = (current_date.month//3) + 1
    year = current_date.year

    quarter_year=[quarter,year]

    current_quarter=quarter_year[0]
    current_year=quarter_year[1]

    #conversions
    end_month=current_quarter*3
    _, last_day=monthrange(year,end_month)
    start_month=(current_quarter*3)-2
    start_day= 1
    start_date=date(year,start_month,start_day)
    end_date=date(year,end_month, last_day)
    quarter_year=[start_date, end_date]

    
    #Determining the quarter
    if last_quarter is None:
        return  quarter_year
    
    if current_year < last_quarter[1]:
        raise ValueError(f" This is anomaly. Current year can't be less than the last year.")
    
    if current_year > last_quarter[1]:
        return quarter_year 
    
    if current_year == last_quarter[1]:
        if last_quarter[0] > current_quarter:
            raise ValueError(f" This is anomaly. Last quarter can't be greater than the current quarter")
        elif current_quarter > last_quarter[0]:
            return quarter_year 
        else:
            return last_quarter
        
    
    
    

def get_latest_dividend_declarations():
    """
    company = Company("AAPL")

    get financials
    financials = company.get_financial()
    financials.income_statement()

    or facts
    xbrl= company.latest_filing(form="10-k").xbrl()
    div_facts =xbrl.factx.filter(concept= "CommonStockDividendPerShare)
    """
    pass


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
    logger.info("Starting to Fecth Dividend Per Share Data=====")

    #Extract + Prep
    df = load_nasdaq_data()
    df = validateInData(df)
    df = extract_columns(df)
    df = normalize_names(df)
    master = build_master_list(df)
    df = match_and_categorize(df, master)

    top_300 = get_top_300(df)
    top_300 = validate_top_300(top_300)

    #Fetch dividend per share prices  + Process
    tickers = validate_top_300(top_300)
    validated_tickers = validate_dividend_tickers(tickers)
    quarters=get_current_quarter(None)
    print(quarters)
    

