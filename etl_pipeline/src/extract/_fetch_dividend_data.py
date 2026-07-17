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
from config.logging_config import setup_logging
from etl_pipeline.src.extract._clean_nasdaq_list import get_nasdaq_list
from etl_pipeline.src.schema.ticker_schemas import CURRENT_PRICE_FILE_SCHEMA
from config.settings import (
    DATA_COLS,
    DIVIDENDS_FILEPATH,
    RAW_SUBDIR
)

RAW_SUBDIR.mkdir(parents=True, exist_ok=True)

logger = get_logger(__name__)
setup_logging()
load_dotenv()

def validate_incoming_tickers(df):
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
    quarter = (current_date.month - 1 )//3 + 1
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
            #If fillings are not yet available, wait for 14 days
            wait_time=current_date-start_date
            wait_time=wait_time.days 
            the_previous_quarter=last_quarter[0]
            the_same_year=last_quarter[1]
            the_end_monthofthe_quarter=the_previous_quarter*3
            _,the_last_dayofthe_quarter=monthrange(the_same_year,the_end_monthofthe_quarter)
            the_start_monthofthe_quarter=(the_previous_quarter*3)-2
            the_start_dayofthe_quarter= 1
            the_start_dateofthe_quarter=date(the_same_year,the_start_monthofthe_quarter, the_start_dayofthe_quarter)
            the_end_dateofthe_quarter=date(the_same_year,the_end_monthofthe_quarter,the_last_dayofthe_quarter)
            the_previous_quarter_period=[the_start_dateofthe_quarter,the_end_dateofthe_quarter]   
            if wait_time < 14:
                return the_previous_quarter_period
            else:
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

def get_latest_dividend_declarations(batch, date_range):
    """
    Pulls dividend data from SEC EDGAR, evaluates time-span durations,
    and normalizes the metrics to a Trailing Twelve Month (TTM) annual scale.
    """
    successful_tickers = []
    failed_tickers = []
    target_tag = "us-gaap:CommonStockDividendsPerShareDeclared"
    start_date = pd.Timestamp(date_range[0])
    
    for batch_number, batch_item in enumerate(batch, start=1):
        for item in batch_item:
            ticker = item['ticker']
            cik = item['cik']
            
            try:
                # 1. INITIALIZATION & EXTRACTION LAYER
                company = Company(cik)
                
                # Fetch a buffer of recent filings (e.g., last 4) 
                # instead of blindly slicing filing[0] to avoid the 0.00 trap
                filings = company.get_filings(form="10-Q")
                
                if not filings:
                    # Fallback for structural zeros
                    successful_tickers.append(pd.DataFrame([{
                        'ticker': ticker, 'cik': cik, 'dividend_per_share': 0.0,
                        'frequency': 'None', 'quarter': (start_date.month - 1)//3 + 1,
                        'year': start_date.year
                    }]))
                    continue
                
                # Gather facts from the most recent filing document
                # (loop over filings if aggregating history)
                xbrl = filings[0].xbrl()
                all_facts = xbrl.query().to_dataframe()
                
                # Filter rows matching the specific XBRL taxonomy concept
                company_df = all_facts[all_facts['concept'] == target_tag][['value', 'period_start', 'period_end']]
                
                if company_df.empty:
                    successful_tickers.append(pd.DataFrame([{
                        'ticker': ticker, 'cik': cik, 'dividend_per_share': 0.0,
                        'frequency': 'None', 'quarter': (start_date.month - 1)//3 + 1,
                        'year': start_date.year
                    }]))
                    continue
                
                # DATA TRANSFORMATION & TIME-SPAN ISOLATION LAYER
                # Convert strings to pandas datetime to enable date mathematics
                company_df['period_start'] = pd.to_datetime(company_df['period_start'])
                company_df['period_end'] = pd.to_datetime(company_df['period_end'])
                
                # Calculate exact disclosure time windows to identify YTD vs Quarter rows
                company_df['duration_days'] = (company_df['period_end'] - company_df['period_start']).dt.days
                
                # Sort Chronologically by period_end so the most recent data points float to the top
                company_df = company_df.sort_values('period_end', ascending=False)
                
                # Isolate the topmost available reporting fact
                latest_fact = company_df.iloc[0]
                days = latest_fact['duration_days']
                raw_value = float(latest_fact['value'])
                
                # DYNAMIC SCALING & NORMALIZATION LAYER
                # Clean the tie-breaker bug and prepare scale for the 3-3 Rule math
                def evaluate_row_metrics(days):

                    # Defensive Check: Handle XBRL Instant facts where period_start is null/NaN
                    if pd.isna(days):
                        return 1, "Quarterly", 4
                    
                    # Priority 1: True Quarter or Single-Day Announcement (Both represent single-quarter rate
                    if (80 <= days <= 100) or (0<= days <=7):
                        return 1, "Quarterly", 4
                    
                    # Priority 2: Cumulative 6-month YTD or true Semi-Annual
                    elif 160 <= days <= 200:
                        return 2, "Semi-Annual", 2
                    # Priority 3: Cumulative Full-Year or true Annual
                    elif 340 <= days <= 380:
                        return 3, "Annual", 1
                    # Priority 4: Irregular reporting periods
                    else:
                        return 4, "Unknown/Irregular",1
                # Map evaluation metrics into temporary helper arrays
                metrics = [evaluate_row_metrics(d) for d in company_df['duration_days']]
                company_df['priority'] = [m[0] for m in metrics]
                company_df['payout_frequency'] = [m[1] for m in metrics]
                company_df['multiplier'] = [m[2] for m in metrics]


                # Sort by period_end DESCENDING (latest date), 
                # then by priority ASCENDING (Priority 1 wins over Priority 2 on the same date)
                company_df = company_df.sort_values(
                    by=['period_end', 'priority'], 
                    ascending=[False, True]
                )
                # Isolate the definitive, highest-priority row
                latest_fact = company_df.iloc[0]
                raw_value = float(latest_fact['value'])
                payout_frequency = latest_fact['payout_frequency']
                annualized_dividend = raw_value * latest_fact['multiplier']


                # LOAD / PACKAGING LAYER
                # Package normalized data back into a structured single-row DataFrame
                output_row = pd.DataFrame([{
                    'ticker': ticker,
                    'cik': cik,
                    'dividend_per_share': annualized_dividend, # Normalized annual metric
                    'raw_payout': raw_value,
                    'frequency': payout_frequency,
                    'quarter': (latest_fact['period_end'].month - 1) // 3 + 1,
                    'year': latest_fact['period_end'].year
                }])
                
                successful_tickers.append(output_row)
                
                # Rate limiting compliance safety delay
                time.sleep(random.uniform(2.0, 4.0))
                
            except Exception as e:
                logger.info(f"Ticker {ticker} failed: {e}")
                failed_tickers.append({'ticker': ticker, 'reason': str(e)})
                
        logger.info(f"Batch {batch_number} processing sequence completed")

    #PIPELINE AGGREGATION & TYPE CASTING
    if successful_tickers:
        final_df = pd.concat(successful_tickers, axis=0, ignore_index=True)
        final_df['dividend_per_share'] = final_df['dividend_per_share'].astype(float)
        logger.info(f"\n== PIPELINE SUCCESS. SHAPE: {final_df.shape}")
        return final_df
    else:
        logger.error("Data pipeline execution returned 0 records.")
        return pd.DataFrame()

def validate_dividend_tickers(dividend_df):
    """
    Validate the output from the get latest dividend declarations."""
    #confirm it it is none.
    if dividend_df is None:
        raise ValueError("No data to validate")
    
    #confirm it is a pandas dataframe
    if not isinstance(dividend_df, pd.DataFrame):
        raise TypeError(f" Expected pandas dataframe got {type(df).__name__}")
    
    #confirm the dataframe is empty
    if dividend_df.empty:
        raise ValueError(f" The dataframe is empty")
    
    #confirm the minimum number of rows to be 400

    if dividend_df.shape[0]<150:
        raise ValueError(f" The dataframe has less than 150 rows which represent 10")
    
    #Confirm the required columns
    required_cols=['ticker','quarter','dividend_per_share']
    missing_col=[]

    for col in required_cols:
        if col not in dividend_df.columns:
            missing_col.append(col)

    if missing_col:
        raise ValueError(f" Missing columns are {missing_col}")
    
    logger.info(f"VALIDATION OF DIVIDEND PER SHARE COMPLETED")
    return dividend_df

def get_dividend_data(nasdaq_list):
    """Checks if data is fresh and orchestrates the entire dividend file"""

    if DIVIDENDS_FILEPATH.is_file():  #Checking if the file exists first
        quarter = get_current_quarter(last_quarter=None)
        current_quarter = pd.Timestamp(quarter[-1]).quarter  #Most recent quarter 
        current_year = quarter[-1].year #Most recent/current year

        #Read only the year and quarter column

        existing = pd.read_csv(
            DIVIDENDS_FILEPATH,
            usecols=["quarter", "year"],
            dtype={"quarter": int, "year": int}
        )

        latest_quarter = existing["quarter"].max()
        latest_year = existing["year"].max()

        if latest_year >= current_year and latest_quarter >= current_quarter:
            logger.info(f"File Found, loading fresh dividend data from the disk....")
            return pd.read_csv(
                DIVIDENDS_FILEPATH,
                dtype={
                    "ticker": str, 
                    "cik" : str, 
                    "dividend_per_share" : str, 
                    "quarter": int, 
                    "year" : int
                }
            )

    #Gather raw materials

    # Fetching dividend prices process
    tickers = validate_incoming_tickers(nasdaq_list)
    date_range = get_current_quarter(last_quarter=[1,2026])
    cik_batches = generate_cik_batches(tickers)
    dividend_data = get_latest_dividend_declarations(cik_batches, date_range)
    dividend_df= validate_dividend_tickers(dividend_data)

    #Saving the fresh dividen data
    fresh_dividend_data = dividend_df 
    fresh_dividend_data.to_csv(
        DIVIDENDS_FILEPATH,
        index= False, 
        float_format= "%.2f",
        na_rep="NA",
        encoding="utf-8"
    )

    logger.info("Pipeline Executed successfuly. FRESH Dividend price data is READY")
    return fresh_dividend_data

if __name__ == "__main__":

    try:
        data_list=get_nasdaq_list()
        dividend_data = get_dividend_data(data_list)
        print("\n=====PIPELINE SUCCESS===")
        print(dividend_data[:50])

    except Exception as e:
        logger.error(f" Pipeline Failed: {str(e)}")

    
    
    

