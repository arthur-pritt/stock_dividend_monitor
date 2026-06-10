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
from config.settings import DATA_COLS

logger = get_logger(__name__)
load_dotenv()
setup_logging()
set_identity(os.environ.get("EDGAR_IDENTITY"))

def validate_incoming_tickers(df):
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
        
def generate_cik_batches(df):
    """"
    Map each ticker tot its respective CIK, generate CIK in batches of 10 and return ticker and CIK as input.
    """

    #Validating incoming data

    if df is None:
        raise ValueError(f" No data provided to generate_cik_batches function")
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a pandas dataframe. Got {type(df).__name__}")
    
    if df.empty:
        raise ValueError(f" The dataframe is empty.")
    
    #Prepring the tickers
    try:
        tickers=(
            df[DATA_COLS['ticker']]
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )

    except KeyError:
        logger.error(f"Unexpected error preparing tickers: {e}")
        raise 

    if not tickers:
        raise ValueError(f" No ticker found during processing")
    #Mapping each ticker to its respective CIK(Central Index Key)

    tickers_cik=[]
    for ticker in tickers:
        try:
            company= Company(ticker)
            tickers_cik.append({
                'ticker': ticker,
                'cik': company.cik
            })
        except Exception as e:
            logger.error(f" Error with {ticker} : {e}")

    #Confirming the number of tickers_cik

    if len(tickers_cik) < 100:
        raise ValueError(f" Expected at least 100 tickers. Got {len(tickers_cik)} out of {len(tickers)}. Failed:{len(tickers_cik)}")
    
    #Generate batches of 10 tickers per batch function

    def ticker_batches(tickers, batch_size=10):
        tickers = iter(tickers)
        while True:
            batch= list(islice(tickers, batch_size))
            if not batch:
                break 
            yield batch 

    tickers_cik_batches = []
    for i, batch in enumerate(ticker_batches(tickers_cik, 10), 1):
        tickers_cik_batches.append(batch)
        logger.info(f"Generate Ticker CIK batch === {i:2d} | Size:{len(batch)}")
    
    logger.info(f"COMPLETED: Generate {len(tickers_cik_batches)} batches")

    return tickers_cik_batches 

def get_latest_earnings_data(batch, date_range):
    """
    A function that pulls the latest earnings per share from SEC EDGAR tool API and returns ticker, cik, earning_per_share, quarter, and year as output
    """

    successful_tickers = []
    failed_tickers =[]
    target_tag = "us-gaap:EarningsPerShareDiluted"
    start_date=pd.Timestamp(date_range[0])
    end_date=pd.Timestamp(date_range[1])

    for batch_number, batch_item in enumerate(batch, start=1):
        for item in batch_item:
            ticker = item['ticker']
            cik = item['cik']

            try:
                #Creating the company object
                company = Company(cik)
                filing = company.get_filings(form="10-Q")
                if not filing:
                    #treat as no earning data
                    # store 0.0 and continue
                    successful_tickers.append(pd.DataFrame([{
                        'ticker' : ticker, 
                        'cik' : cik,
                        'earnings_pershare_diluted' :0.0,
                        'quarter' :(start_date.month -1)//3 + 1,
                        'year' : start_date.year
                    }]))
                    continue 
                filing = filing[0]
                xbrl= filing.xbrl()

                #Get all the facts
                all_facts = xbrl.query().to_dataframe()

                #Filter by concept using pandas
                company_df = all_facts[
                    all_facts['concept'] == target_tag
                ][['value', 'period_start', 'period_end']]

                #Keep by concept using pandas
                company_df = company_df.sort_values('period_end', ascending=False).head(1)

                #Converting to pandas date
                company_df['period_start']= pd.to_datetime(company_df['period_start'])
                company_df['period_end'] = pd.to_datetime(company_df['period_end'])

                #Renaming columns
                company_df['ticker'] = ticker 
                company_df['cik']= cik 
                company_df['quarter']=company_df['period_end'].apply(lambda x: (x.month- 1)//3 + 1)
                company_df['year'] = company_df['period_end'].apply(lambda x: x.year)
                company_df['earnings_pershare_diluted'] = company_df['value']
                company_df=company_df[['ticker', 'cik', 'earnings_pershare_diluted', 'quarter', 'year']]

                #Appending/storing columns to successful_tickers dataframe
                successful_tickers.append(company_df)

                #Safely delay between batches
                time.sleep(random.uniform(2.0,4.0))


            except Exception as e:
                logger.info(f" Batch {batch_number} failed: {e}")
                failed_tickers.append({
                    'ticker':ticker,
                    'reason' : str(e)
                })
        logger.info(f"Batch {batch_number} successful")

    if successful_tickers:
        successful_tickers = pd.concat(successful_tickers, axis=0, ignore_index=True)
        successful_tickers['earnings_pershare_diluted']= successful_tickers['earnings_pershare_diluted'].astype(float)
        logger.info(f"\n=== COMPLETED! {successful_tickers.shape[0]}")

    else:
        logger.info(f"No data was downloaded")

    logger.info(f"\n DOWNLOADED COMPLETED")
    logger.info(f" Successful batches: {successful_tickers.shape[0]}/300")

    if failed_tickers:
        logger.info(f" Failed batches: {failed_tickers}")

    return successful_tickers

def validate_earnings_tickers(earning_df):
    """
    Validate the output from the latest earning per share tickers"""

    #confirm it is None
    if  earning_df is None:
        raise ValueError("No data to validate")
    
    #confirm the dataframe is pandas dataframe
    if not isinstance(earning_df, pd.DataFrame):
        raise TypeError(f" Expectec pandas dataframe got {type(df).__name__}")
    
    #confirm the dataframe is empty
    if earning_df.empty:
        raise ValueError(f" The dataframe is empty")
    
    #confirm the minimum number of rows to be 400
    if earning_df.shape[0]<150:
        raise ValueError(f" The datafraame has less than 150 rows which represent 10")
    
    #confirm the required columns
    required_cols=['ticker','quarter','earnings_pershare_diluted']
    missing_col=[]

    for col in required_cols:
        if col not in earning_df.columns:
            missing_col.append(col)

    if missing_col:
        raise ValueError(f" Missing columns are {missing_col}")

    logger.info(f"VALIDATION OF EARNINGS PER SHARE DILUTED COMPLETED")
    return earning_df

def get_earning_data():
    """
    Facade function that orchestrates the main earning data file."""

    # Gather all the raw materials
    final_list= get_nasdaq_list()

    # Fetching earning data prices
    tickers = validate_incoming_tickers(final_list)
    current_quarter = get_current_quarter(last_quarter=[1,2026])
    cik_batches = generate_cik_batches(tickers)
    earnings_data = get_latest_earnings_data(cik_batches,current_quarter)
    validated_earning_data =validate_earnings_tickers(earnings_data)

    logger.info(f" Pipeline Executed Successfully. Earning data is READY.")
    return validated_earning_data

if __name__ == "__main__":
    
    try:
        earning_data = get_earning_data()
        print("\n====PIPELINE SUCCESS====")
        print(earning_data)

    except Exception as e:
        logger.error(f" Pipeline Failed: {str(e)}")
