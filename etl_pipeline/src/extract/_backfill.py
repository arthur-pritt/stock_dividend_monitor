import pandas as pd
import pandera.pandas as pa
from yahooquery import Ticker
import time
import pandas_market_calendars as mcal
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

def count_trading_days(start_date, end_date, calendar_name: str = "NYSE") -> int:
    """Count actual NYSE trading days between two dates (inclusive)."""
    if pd.to_datetime(start_date) > pd.to_datetime(end_date):
        return 0
    
    calendar = mcal.get_calendar(calendar_name)
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")
    
    valid_days = calendar.valid_days(start_date=start_str, end_date=end_str)
    return len(valid_days)

def _get_last_63_trading_days() -> tuple[str, str]:
    """Calculate exact start and end dates for the last 63 NYSE trading days."""
    nyse = mcal.get_calendar("NYSE")
    today = pd.Timestamp.now(tz="UTC").normalize()

    # Generous buffer to guarantee at least 63 trading days
    buffer_days = pd.Timedelta(days=120)   # ~3 months calendar days
    start_search = (today - buffer_days).strftime("%Y-%m-%d")

    valid_days = nyse.valid_days(
        start_date=start_search,
        end_date=today.strftime("%Y-%m-%d")
    )

    # Take the most recent 63 trading days (inclusive)
    if len(valid_days) < 63:
        # Rare fallback with bigger buffer
        start_search = (today - pd.Timedelta(days=200)).strftime("%Y-%m-%d")
        valid_days = nyse.valid_days(start_date=start_search, end_date=today.strftime("%Y-%m-%d"))

    start_date = valid_days[-63]   # 63rd trading day back
    end_date   = valid_days[-1]    # most recent trading day

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def validate_tickers(df):
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Input must be a pandas dataframe. got {type(df).__name__}")
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return TICKER_SCHEMA.validate(df)

def fetch_raw_data(df):
    """Fixed version: fetches ~63 trading days reliably, forces UTC, minimal index magic."""
    if df is None or df.empty:
        logger.error("No dataframe provided to fetch_raw_data")
        return None

    # Prepare symbols (keep original case - yahooquery is case-insensitive)
    symbols = df[DATA_COLS['ticker']].astype(str).str.strip().tolist()

    # Generate batches of 10
    def ticker_batches(tickers, batch_size=10):
        tickers = iter(tickers)
        while True:
            batch = list(islice(tickers, batch_size))
            if not batch:
                break
            yield batch

    all_batches = []
    start_str, end_str = _get_last_63_trading_days()

    logger.info(f"Fetching last 63 trading days: {start_str} to {end_str}")

    for i, batch in enumerate(ticker_batches(symbols, 10), 1):
        logger.info(f"Processing=======Batch {i}")

        success = False
        for attempt in range(3):
            try:
                #explicit start/end
                batch_df = Ticker(batch).history(
                    start=start_str,
                    end=end_str,
                    interval='1d'
                )

                if batch_df is None or batch_df.empty:
                    raise ValueError("Empty response from yahooquery")

                # === Simple and robust timezone fix ===
                # Reset index to turn MultiIndex (symbol, date) into columns
                batch_df = batch_df.reset_index()

                # Force date column to be clean UTC-aware (this kills mixed timezone errors)
                batch_df['date'] = pd.to_datetime(batch_df['date'], utc=True)

                # Keep only the columns you need for cleaning
                keep_cols = ['symbol', 'date', 'adjclose', 'open', 'high', 'low', 'close', 'volume']
                batch_df = batch_df[[c for c in keep_cols if c in batch_df.columns]]

                all_batches.append(batch_df)
                logger.info(f"Batch {i}: successful on attempt {attempt + 1}")
                success = True
                break

            except Exception as e:
                wait_time = (3 * (2 ** attempt)) + random.uniform(0, 0.4)
                if attempt < 2:
                    logger.warning(f"Attempt {attempt + 1} failed for batch {i}. "
                                   f"Retrying in {wait_time:.2f}s... Error: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Batch {i}: FAILED AFTER 3 ATTEMPTS. Pipeline moves on.")
                    logger.error(f"Batch {i} failed: {e}")

        if success:
            time.sleep(1)  # polite delay between successful batches

    if not all_batches:
        logger.error("No data was collected from any batch")
        return None

    logger.info("Glueing all batches together.......")
    master_df = pd.concat(all_batches, ignore_index=True, sort=False)

    # Final cleanup
    master_df = master_df.dropna(subset=['adjclose'])
    master_df = master_df.sort_values(['symbol', 'date'])

    logger.info(f"fetch_raw_data completed → {len(master_df)} rows, "
                f"{master_df['symbol'].nunique()} unique tickers")

    print(type(master_df))
    return master_df




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

    return df_clean
                          
    

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
    logger.info(result_2)
    #logger.info(f"Result shape: {result.shape}")
    #logger.info(f"Columns: {result.columns.tolist()}")
    #logger.info(result)



    


    




