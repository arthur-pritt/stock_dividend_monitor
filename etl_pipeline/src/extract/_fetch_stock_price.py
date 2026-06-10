import pandas as pd
import yfinance as yf
import  time
import pandera.pandas as pa
import pandas_market_calendars as mcal
import random 
from itertools import islice
from datetime import datetime, timedelta

from etl_pipeline.src.schema.ticker_schemas import TICKER_SCHEMA,CURRENT_PRICE_FILE_SCHEMA
from etl_pipeline.src.extract._smart_session import RobustCurlSession
from config.logging_config import setup_logging
from etl_pipeline.src.extract._download_nasdaq_list import load_nasdaq_data
from etl_pipeline.src.extract._clean_nasdaq_list import(
    validate_top_300,validate_top_300,
    extract_columns,validateInData,
    normalize_names,
    build_master_list,
    match_and_categorize,
    get_top_300)

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
        candidate_date= schedule.copy()
    
    candidate_date=candidate_date[['date', 'market_close']]
    
    invalid_streak= 0
    maxi_attempts= 14
    valid_days= 0
    max_days= 2
    valid_dates=[]
    curr_date = reference_dt

    while valid_days < max_days:
        #checking today
        if valid_days == 0 and candidate_date.empty is not None:
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
        schedule=nyse.schedule(start_date=curr_date.date(),end_date=curr_date.date())
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

    #logger.info(f"Recent valid two trading days: {valid_dates}")

    #Extracting the date Timestamps before returning
    return [entry['date'] for entry in valid_dates]


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

def generate_batches(df):
    """Generate ticker symbols in batches"""

    #Validating incoming data

    if df is None or df.empty:
        logger.error("No dataframe provided to generate_batches")
        return None 
    
    #Preping the tickers
    try:
        symbols=(
            df[DATA_COLS['ticker']]
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
    except KeyError:
        logger.error(f"Column {DATA_COLS['ticker']} not found in the database")
        return None 
    except Exception as e:
        logger.error(f"Unexpected error preparing tickers: {e}")
        return None 
    if not symbols:
        logger.warning(f"No ticker found during processing")
        return []
    #Generate batches of 10 function

    def ticker_batches(tickers, batch_size=10):
        tickers = iter(tickers)
        while True:
            batch = list(islice(tickers, batch_size))
            if not batch:
                break
            yield batch 

    all_batches= []

    for i, batch in enumerate(ticker_batches(symbols,10),1):
        all_batches.append(batch)
        logger.info(f"Generated Batch ==== {i:2d} | Size: {len(batch)}")
    
    logger.info(f"COMPLETED: Generated {len(all_batches)} batches")
    return all_batches 

def fetch_adjusted_close(df):
    "Fetching daily adjusted close price and returning a dataframe."

    #Creating/initiliazing the custom session
    curl_session = RobustCurlSession(
        impersonate= "chrome131",
        delay_min=1.0,
        delay_max=2.
    )

    #Store tickers from all batches
    ticker_results= []
    failed_batches=[]

    logger.info("Starting download for 30 batches...\n")

    for batch_number, tickers in enumerate(df, start=1):
        #logger.info(f"Processing Batch {batch_number}/30 | Tickers:{len(tickers)}")
        

        try:
            #Geting the two recent trading days
            last_two_days=recent_two_trading_days()

            #Preping the start and end dates
            start_date=last_two_days[1]
            end_date=last_two_days[0] + timedelta(days=1)

            #Downloading the data
            data=yf.download(
                tickers=tickers,
                start=start_date,
                end=end_date,
                interval="1d",
                auto_adjust=True,
                threads=False,
                session=curl_session.session,
                progress=False
            )

            #Extract Adjusted Close

            if not data.empty:
                batch_long = data['Close'].stack().reset_index()
                batch_long.columns= ['Date', 'Ticker', 'Adj_Close']
                
                # Store the result
                ticker_results.append(batch_long)

                logger.info(f" Batch {batch_number} succesful")

            else:
                logger.info(f"Batch {batch_number} returned no data")

        except Exception as e:
            logger.info(f" Batch {batch_number} failed: {e}")
            failed_batches.append(batch_number)

        #Safety delay between batches
        time.sleep(random.uniform(2.0,4.0))


    if ticker_results:
        ticker_prices_df= pd.concat(ticker_results, axis=0, ignore_index=True)
        ticker_prices_df= ticker_prices_df.sort_values(['Date','Ticker']).reset_index(drop=True)

        logger.info(f"\n== COMPLETED! {ticker_prices_df.shape[1]}")
    else:
        logger.info("No data was downloaded")
    
    logger.info("\n Donwload Completed")
    logger.info(f" Successful batches: {len(ticker_results)}/30")

    if failed_batches:
        logger.info(f" Failed batches: {failed_batches}")
    
    return ticker_prices_df

def clean_ticker_prices(df):
    """
    Final cleaned version - Produces clean long-format data.
    Output columns: date, ticker, adj_close
    """
    if df is None or df.empty:
        raise ValueError("No data to clean. DataFrame is empty or None.")

    print("=== Starting Final Data Cleaning ===")
    print("Original shape:", df.shape)

    # 1. Convert Date to datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # 2. Sort the data properly
    df = df.sort_values(['Date', 'Ticker']).reset_index(drop=True)

    # 3. Select and rename columns cleanly
    final_df = df[['Date', 'Ticker', 'Adj_Close']].copy()
    final_df.columns = ['date', 'ticker', 'adj_close']   # Consistent lowercase
    final_df['year'] = final_df['date'].dt.year
    final_df['month'] = final_df['date'].dt.month

    # 4. Final validation and summary
    print("\n=== Cleaning Summary ===")
    print("Final Shape          :", final_df.shape)
    print("Unique Tickers       :", final_df['ticker'].nunique())
    print("Date Range           :", final_df['date'].min(), "→", final_df['date'].max())
    print("Total NaNs           :", final_df.isnull().sum().sum())
    
    print("\nNumeric Summary (Adj_Close):")
    print(final_df['adj_close'].describe())

    print("\nFirst 5 rows of cleaned data:")
    print(final_df.head())

    print("\nData cleaning completed successfully!")
    
    return final_df

def validating_clean_tickers(clean_df):
    """Validating the output from the clean ticker price function."""

    #Confirm it is not none
    if clean_df is None:
        raise ValueError("No data to validate")
    
    #Confirm it is a pandas dataframe
    if not isinstance(clean_df, pd.DataFrame):
        raise TypeError(f"Expected pandas dataframe got {type(clean_df).__name__}")
    
    #Confirm if the the dataframe is empty

    if clean_df.empty:
        raise ValueError(f" The dataframe has no values. It's empty")
    
    #Confirm the minimum number of rows to be 400

    if clean_df.shape[0]<400:
        raise ValueError(f" The dataframe has less than 400 rows which rep 100 tikers. got: {clean_df.shape[0]}")
    
    #confirm the required columns
    required_cols=['ticker','date','adj_close']
    missing_col= []
    for col in required_cols:
        if col not in clean_df.columns:
            missing_col.append(col)

    if missing_col:
        raise ValueError(f"Missing columns are {missing_col}")
    
    logger.info(f"VALIDATION OF TICKER ADJUST CLOSE COMPLETED")

    return clean_df

def get_price_data():
    """
    Facade function that orchestrates the entire stoc price file."""

    logger.info("Starting to Fetch the Current Closing DAY prices=========")

    # Gather the raw materials
    raw_data = load_nasdaq_data()
    validated_data = validateInData(raw_data)
    extracted_data = extract_columns(validated_data)
    normalized_data = normalize_names(extracted_data)
    master_list = build_master_list(normalized_data)
    categorized_list = match_and_categorize(normalized_data, master_list)
    top_300 = get_top_300(categorized_list)
    final_list = validate_top_300(top_300)

    # Fetching stock price process
    tickers = validate_tickers(final_list)
    valid_days, _= count_nyse_trading_days('2025-01-01','2026-07-10',inclusive=True)
    candidate_days = recent_two_trading_days()
    batches= generate_batches(tickers)
    ticker_prices=fetch_adjusted_close(batches)
    clean_prices=clean_ticker_prices(ticker_prices)
    validated_price_data = validating_clean_tickers(clean_prices)
    
    logger.info("Pipeline Executed successfuly. Stock Price Data is READY")
    return validated_price_data

if __name__ == "__main__":
    try:
        stock_price_data = get_price_data()
        print("\n=====PIPELINE SUCCESS====")
        print(stock_price_data)

    except Exception as e:
        logger.error(f" Pipeline Failed: {str(e)}")
    
    
    
    

    


    


    



