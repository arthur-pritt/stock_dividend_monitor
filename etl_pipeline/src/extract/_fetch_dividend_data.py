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
from etl_pipeline.src.extract._standardization_setup import build_standardization_context


logger = get_logger(__name__)
load_dotenv()
set_identity(os.environ.get("EDGAR_IDENTITY"))



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

    current_quarter=quarter
    current_year=year

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
            the_quarter=last_quarter[0]
            the_year=last_quarter[1]
            the_end_month=the_quarter*3
            _, the_last_day=monthrange(the_year,the_end_month)
            the_start_month=(the_quarter*3)-2
            the_start_day= 1
            the_start_date=date(the_year,the_start_month,the_start_day)
            the_end_date=date(the_year, the_end_month,the_last_day)

            previous_period=[the_start_date,the_end_date]

            return previous_period
        
def generate_cik_batches(df):
    """
    Map each ticker to its respective CIK, generate CIK in batches of 10
    and return ticker and CIK as output."""

    #Validating incoming data

    if df is None: 
        raise ValueError(f" No data provided to generate_cik_batches function")
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f" Expected a pandas dataframe. Got {type(df).__name__}")
    
    if df.empty:
        raise ValueError(f" The dataframe is empty.")
    
    #Preping the tickers

    try:
        tickers=(
            df[DATA_COLS['ticker']]
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
    except KeyError:
        logger.error(f"Column {DATA_COLS['ticker']} not found in the database")
        raise
    
    except Exception as e:
        logger.error(f"Unexpected error preparing tickers: {e}")
        raise
    
    if not tickers:
        raise ValueError(f"No ticker found during processing")
    #Mapping the each ticker to its  respective CIK(Central Index Key)

    tickers_cik=[]
    for ticker in tickers:
        try:
            company=Company(ticker)
            tickers_cik.append({
                'ticker' : ticker,
                'cik'  : company.cik
            })
        except Exception as e:
            logger.error(f" Error with {ticker} : {e}")

    #Confirming the number of tickers_cik

    if len(tickers_cik) < 100:
        raise ValueError(f"Expected at least 100 tickers. Got {len(tickers_cik)} out of {len(tickers)}. Failed:{len(tickers)-len(tickers_cik)}")
    
    #Generate batches of 10 tickers per batch function

    def ticker_batches(tickers, batch_size=10):
        tickers = iter(tickers)
        while True:
            batch=list(islice(tickers,batch_size))
            if not batch:
                break
            yield batch 
    
    tickers_cik_batches= []
    for i, batch in enumerate(ticker_batches(tickers_cik,10),1):
        tickers_cik_batches.append(batch)
        logger.info(f"Generated Ticcker CIK Batch === {i:2d} | Size: {len(batch)}")

    logger.info(f"COMPLETED: Generate {len(tickers_cik_batches)} batches")

    return tickers_cik_batches

def retry(times=3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == times - 1:
                        raise 
                    time.sleep(2 ** attempt)
        return wrapper
    return decorator
@retry(times=3)
def get_latest_dividend_declarations(batch, date_range):
    """
    """
    successful_tickers = []
    failed_tickers =[]
    target_tag="us-gaap:CommonStockDividendsPerShareDeclared"

    for batch_number, item in enumerate(batch, start=1):
        ticker = item['ticker']
        cik = item['cik']
        start_date=date_range[0]
        end_date=date_range[1]
        try:
            company = Company(cik)
            filing = company.get_filings(form="10-Q")[0]
            xbrl=filing.xbrl()
            company_df=(xbrl.query()
                        .by_statement_type("StatementOfEquity")
                        .by_concept(target_tag)
                        .by_dimension(None)
                        .by_per
                        .to_dataframe('value','period_start','period_end','fiscal_period','fiscal_year'))
            #converting fiscal dates to calendar dates
            company_df = company_df[(company_df['period_start'] >= start_date) &(company_df['period_end'] <= end_date)]
            #Renaming columns
            company_df['ticker'] = ticker
            company_df['cik'] = cik
            company_df['quarter'] = company_df['period_start'].apply(lambda x: (x.month // 3) + 1)
            company_df['year'] = company_df['period_start'].apply(lambda x: x.year)
            company_df['dividend_per_share'] = company_df['value']
            company_df = company_df[['ticker', 'cik', 'dividend_per_share', 'quarter', 'year']]
            successful_tickers.append(company_df)
            logger.info(f" Batch {batch_number} succesful")
        except Exception as e:
            logger.info(f" Batch {batch_number} failed: {e}")
            failed_tickers.append(batch_number)
        
        #Safety delay between batches
        time.sleep(random.uniform(2.0,4.0))

        if successful_tickers:
            successful_tickers= pd.concat(successful_tickers, axis=0, ignore_index=True)
            #successful_tickers= successful_tickers_df.sort_values(['ticker','cik']).reset_index(drop=True)
            logger.info(f"\n== COMPLETED! {successful_tickers_df.shape[1]}")

        else:
            logger.info("No data was downloaded")
        
        logger.info("\n Donwload Completed")
        logger.info(f" Successful batches: {len(successful_tickers)}/30")

        if failed_tickers:
            logger.info(f" Failed batches: {failed_tickers}")
        
        return successful_tickers


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
    date_range=get_current_quarter(last_quarter=None)
    cik_batches=generate_cik_batches(validated_tickers)
    dividend_data=get_latest_dividend_declarations(cik_batches,date_range)
    print(dividend_data)
    
    
    

