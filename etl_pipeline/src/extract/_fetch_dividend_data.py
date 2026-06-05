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
    """
    successful_tickers = []
    failed_tickers =[]
    target_tag = "us-gaap:CommonStockDividendsPerShareDeclared"
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
                    # treat as no dividend data
                    # store 0.0 and continue
                    successful_tickers.append(pd.DataFrame([{
                        'ticker' : ticker,
                        'cik' : cik,
                        'dividend_per_share' : 0.0,
                        'quarter':(start_date.month - 1)//3 + 1,
                        'year' : start_date.year
                    }]))
                    continue
                filing = filing[0]
                xbrl=filing.xbrl()
                
                # Get all facts
                all_facts = xbrl.query().to_dataframe()

                # Filter by concept using pandas
                company_df = all_facts[
                    all_facts['concept'] == target_tag
                    ][['value', 'period_start', 'period_end']]
                
                # Keep only most recent declaration
                company_df = company_df.sort_values('period_end', ascending=False).head(1)

                # For companies that don't declare dividend will have 0.0 as their value
                if company_df.empty:
                    company_df= pd.DataFrame([{
                        'ticker': ticker,
                        'cik':cik,
                        'dividend_per_share': 0.0,
                        'quarter':(start_date.month - 1 )//3+1,
                        'year':start_date.year
                    }])
                    successful_tickers.append(company_df)
                    continue
                
                #Converting to pandas date
                company_df['period_start']= pd.to_datetime(company_df['period_start'])
                company_df['period_end']= pd.to_datetime(company_df['period_end'])

                #Renaming columns
                company_df['ticker'] = ticker
                company_df['cik'] = cik
                company_df['quarter'] = company_df['period_end'].apply(lambda x: (x.month - 1 )// 3 + 1)
                company_df['year'] = company_df['period_end'].apply(lambda x: x.year)
                company_df['dividend_per_share'] = company_df['value']
                company_df = company_df[['ticker', 'cik', 'dividend_per_share', 'quarter', 'year']]


                #Appending/storing columns to successful_tickers dataframe
                successful_tickers.append(company_df)


                #Safety delay between batches
                time.sleep(random.uniform(2.0,4.0))

            except Exception as e:
                logger.info(f" Batch {batch_number} failed: {e}")
                failed_tickers.append({
                    'ticker': ticker,
                    'reason' :str(e)
                    })
        logger.info(f"Batch {batch_number} successful")
                

    if successful_tickers:
        successful_tickers= pd.concat(successful_tickers, axis=0, ignore_index=True)
        successful_tickers['dividend_per_share'] = successful_tickers['dividend_per_share'].astype(float)
        #successful_tickers= successful_tickers.sort_values(['ticker','cik']).reset_index(drop=True)
        logger.info(f"\n== COMPLETED! {successful_tickers.shape[0]}")

    else:
        logger.info("No data was downloaded")
        
    logger.info(f"\n DOWNLOAD COMPLETED")    
    logger.info(f" Successful batches: {successful_tickers.shape[0]}/300")

    if failed_tickers:
        logger.info(f" Failed batches: {failed_tickers}")
        
    return successful_tickers

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
    validated_tickers = validate_incoming_tickers(top_300)
    date_range=get_current_quarter(last_quarter=[1,2026])
    cik_batches=generate_cik_batches(validated_tickers)
    dividend_data=get_latest_dividend_declarations(cik_batches,date_range)
    dividend_df=validate_dividend_tickers(dividend_data)
    print(dividend_data)
    print(dividend_df)

    
    
    

