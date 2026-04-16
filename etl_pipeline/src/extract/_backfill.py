import pandas as pd
import pandera.pandas as pa
from yahooquery import Ticker
import time
import requests
import random
from itertools import islice



from etl_pipeline.src.schema.ticker_schemas import TICKER_SCHEMA
#importing config files
from config.logging_config import get_logger
from config.settings import (
    DATA_COLS
)

#Getting the logger for the module
logger=get_logger(__name__)

def validate_tickers(df):
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Input must be a pandas dataframe. got {type(df).__name__}")
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return TICKER_SCHEMA.validate(df)

def fetch_raw_data(df):
    #Preping the data
    symbols=df[DATA_COLS['ticker']]
    symbols_list=symbols.to_list() #Converting to a list
    lower_case_symbols=[symbol.lower() for symbol in symbols_list]
    symbols_list=lower_case_symbols #converting the ticker values into lower case

    #Generate batches of 10 tickers
    def ticker_batches(tickers,batch_size=10):
        tickers = iter(tickers)
        while True:
            batch =list(islice(tickers,batch_size))
            if not batch:
                break
            yield batch
    all_batches=[]
    
    for i, batch in enumerate(ticker_batches(symbols_list,10),1):
        logger.info(f"Processing=======Batch {i} ")#Tickers:{', '.join(batch)}
        success = False

        for attempt in range(3):
            try:
                #fetching the 90 day historical data with 1 day interval
                batch_df=Ticker(batch).history(period='3m',interval='1d')
                if batch_df is not None and not batch_df.empty:
                    logger.info(f"Batch {i}:successful on attempt {attempt + 1}")
                    # Standardize the MultiIndex before appending
                    idx = batch_df.index.to_frame()
                    # Ensure level 1 (dates) is clean UTC
                    idx.iloc[:, 1] = pd.to_datetime(idx.iloc[:, 1]).dt.tz_localize(None).dt.tz_localize('UTC')
                    batch_df.index = pd.MultiIndex.from_frame(idx)

                    all_batches.append(batch_df)
                    success = True
                    break
                else:
                    raise ValueError("Empty Data")
            
            except Exception as e:
                wait_time=(3*(2**attempt)) + random.uniform(0,0.4)
                if attempt <2:
                    logger.warning(f"Attempt {attempt + 1} failed for batch {i}."
                                   f"Retrying in {wait_time:.2f}s... Error:{e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Batch {i}:FAILED AFTER 3 ATTEMPTS. Pipelines Moves on")
                    #catch failures but keep looping for the next batch
                    logger.error(f"Batch {i} failed:{e}")
        if success:
            time.sleep(1)
    if all_batches:
        logger.info("Glueing all batches together.......")
        master_df=pd.concat(all_batches, sort=False)
        print(type(master_df))
        #print(master_df[0:50])
        return master_df
    else:
        logger.error(f" No data was collected from any batch")
        return None

def clean_raw_data(df):
    results = {}
    df_clean = df.reset_index()
    df_clean.columns = [str(c).lower() for c in df_clean.columns]

    # --- 1. CLEANING & RENAMING ---
    if 'dividends' in df_clean.columns:
        df_clean = df_clean.drop(columns=['dividends'])
    
    if 'symbol' not in df_clean.columns:
        for col in ['level_0', 'index', 'symbol']:
            if col in df_clean.columns:
                df_clean = df_clean.rename(columns={col: 'symbol'})
                break

    if 'date' not in df_clean.columns:
        if 'level_1' in df_clean.columns:
            df_clean = df_clean.rename(columns={'level_1': 'date'})

    # --- 2. THE SAFETY GATE ---
    if 'adjclose' not in df_clean.columns:
        return None, {"is_empty": True, "error": "Not a price dataframe"}

    # --- 3. THE MAGIC INGREDIENTS (Add these now!) ---
    # Convert to datetime so .dt.date works
    df_clean['date'] = pd.to_datetime(df_clean['date'], utc=True)
    
    # Drop the 2,569 weekend/holiday nulls
    df_clean = df_clean.dropna(subset=['adjclose'])

    # --- 4. VALIDATION MATH ---
    results['is_empty'] = df_clean.empty
    results['total_rows'] = len(df_clean) 
    
    if 'symbol' in df_clean.columns and 'date' in df_clean.columns:
        results['unique_tickers'] = df_clean['symbol'].nunique()
        
        results['trading_days'] = df_clean['date'].dt.date.nunique()
        results['duplicate_keys'] = int(df_clean.duplicated(subset=['symbol', 'date']).sum())
        
        today_utc = pd.Timestamp.now(tz='UTC')
        results['future_date_count'] = int((df_clean['date'] > today_utc).sum())
    
    results['null_row_count'] = int(df_clean.isna().any(axis=1).sum())
    results['duplicate_rows'] = int(df_clean.duplicated().sum())

    return df_clean, results
                          
    

if __name__ == "__main__":
    from config.logging_config import setup_logging
    from etl_pipeline.src.extract._clean_nasdaq_data import (
        validateInData, extract_columns, normalize_names,
        build_master_list, match_and_categorize,
        get_top_300, validate_top_300, pre_validate_with_yahoo
    )
    from etl_pipeline.src.extract._download_nasdaq_list import load_nasdaq_data

    setup_logging()
    logger.info("Testing validate_tickers...")

    # Run the chain to get data
    df = load_nasdaq_data()
    validated_data = validateInData(df)
    final_three_columns = extract_columns(validated_data)
    normalized_df = normalize_names(final_three_columns)
    master_reference = build_master_list(normalized_df)
    final_categorized_df = match_and_categorize(normalized_df, master_reference)
    top_300 = get_top_300(final_categorized_df)
    validated_top_300 = validate_top_300(top_300)

    # Now test YOUR function
    result = validate_tickers(validated_top_300)
    result_2= fetch_raw_data(result)
    #logger.info(result_2)
    #logger.info(f"Result shape: {result.shape}")
    #logger.info(f"Columns: {result.columns.tolist()}")
    #logger.info(result)



    


    




