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

    logger.info(f"Counting trading days from {start_date} to {end_date} | inclusive={inclusive} | calendar={calendar_name}")

    #Step 0: Validate: if start_date is greater than end date, the system raises  valuerror.
    if start_date > end_date:
        logger.error(f"Validation failed: start_date ({start_date}) > end_date ({end_date})")
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
        result = 1 if (valid_trading_days == 1 and inclusive) else 0
        logger.info(f"Same day case → Result: {result}")
        return result, trading_dates[:result]
        
    #Step 6: Handle inclusive= False for different dates
    if not inclusive and valid_trading_days > 0:
        dates_str=trading_dates.strftime('%Y-%m-%d')
        original = valid_trading_days

        # Safe string conversion
        start_date=str(start_date)[:10]
        end_date  =str(end_date)[:10]

        if dates_str[0]==start_date:
            valid_trading_days -=1
            logger.debug(f"Excluded start_date: {start_date}")

        if valid_trading_days > 0 and dates_str[-1]== end_date:
            valid_trading_days -= 1
            logger.debug(f"Excluded end_date: {end_date}")
        
        logger.debug(f"inclusive=False: Reduced from {original} to {valid_trading_days}")

    
    logger.info(f"Final trading days count: {valid_trading_days}")
    return valid_trading_days, trading_dates

def recent_two_trading_days():
    """
    Functions that pulls the two recent or last trading days
    """

    #Setting the current time & converting to UTC
    curr_nys_time=pd.Timestamp.now(tz='America/New_York')

    #Creating a reference date
    reference_dt=curr_nys_time

    #Creating the NYSE Object Calendar
    nyse= mcal.get_calendar('NYSE')
    
    attempt = 0
    max_attempts = 10
    market_close_found = False 
    while market_close_found == False:
        #safety limits
        attempt += 1
        if attempt >max_attempts:
            raise RuntimeError(f"CRITICAL: Failed to find a valid trading day after 10 attempts")
        
        #Get NYSE calendar schedule
        #date() to pass a clean "calendar page" instead of a "cloak time"
        schedule=nyse.schedule(start_date=reference_dt.date(),end_date=reference_dt.date())

        #Reseting the index to get the date column
        schedule= schedule.reset_index()
        schedule=schedule.rename(columns={'index':'date'})

        #Holiday check
        if schedule.empty:
            reference_dt -= pd.Timedelta(days=1)#Go back I day
            continue 

        #Time check
        actual_market_close= schedule['market_close'].iloc[0]
        
        #Check if today and compare current time
        is_today = reference_dt.date() == curr_nys_time.date()

        if is_today and (curr_nys_time<actual_market_close):
            reference_dt -= pd.Timedelta(days=1)
            continue 

        market_close_found= True 
        candidate_date= schedule.copy
        candidate_date=candidate_date[['date', 'market_close']]
    
    invalid_streak= 0
    maxi_attempts= 14
    valid_days= 0
    max_days= 2
    valid_dates =[]
    valid_days_found= False 
    curr_date = reference_dt

    while valid_days < max_days:
        #checking today
        if valid_days == 0 and not candidate_date.empty:
            #append the valid day to the list
            valid_dates.append({
                'date': candidate_date['date'].iloc[0],
                'market_close':candidate_date['market_close'].iloc[0]
            })

            valid_days += 1
            invalid_streak = 0
            curr_date -= pd.Timedelta(days=1)
            continue

        #Fetching Next candidate date
        schedule=nyse.schedule(start_date=curr_date.date(),end_date=curr_date)
        candidate_date= schedule.reset_index()
        candidate_date=candidate_date.rename(columns={'index':'date'})

        if not candidate_date.empty:
            valid_dates.append({
                'date':candidate_date['date'].iloc[0],
                'market_close':candidate_date['market_close'].iloc[0]
            })
            valid_days +=1
            invalid_streak = 0

        else:
            #Invalid day(Holiday/Weekend)
            invalid_streak += 1

        if invalid_streak > maxi_attempts:
            raise RuntimeError(f"CRITICAL: {invalid_streak}consecutive invalid days. Something is wrong with the data or calendar")
        
        curr_date=curr_date - pd.Timedelta(days=1)

    logger.info(f"Recent valid two trading days: {valid_dates}")
    
    return valid_dates


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
    valid_days, _= count_nyse_trading_days('2025-01-01', '2026-01-10', inclusive=True)
    candidate_date=recent_two_trading_days()
    print(tickers.info)
    print(valid_days)
    print(candidate_date)


    



