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
    symbols=[symbol.lower() for symbol in symbols]

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
    # yahooquery end is exclusive so add 1 calendar day

    end_date_exclusive = (
    pd.to_datetime(end_str) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")


    logger.info(f"Fetching last 63 trading days: {start_str} to {end_str}")
    logger.info(f"Note: end date {end_str} may not be available yet if market is open or data unpublished")

    for i, batch in enumerate(ticker_batches(symbols, 10), 1):
        logger.info(f"Processing=======Batch {i}")

        success = False
        for attempt in range(3):
            try:
                #explicit start/end
                batch_df = Ticker(batch).history(
                    start=start_str,
                    end=end_date_exclusive,
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

    return master_df

def clean_and_validate(df: pd.DataFrame, min_days_threshold: int = 55):
    """
    Cleans and validates stock data.

    Returns:
    - Cleaned DataFrame
    - Key validation results (minimal, decision-focused)
    """

    # --- 0. EARLY EXIT ---
    if df is None:
        raise ValueError("No data provided")
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected DataFrame got {type(df).__name__}")
    
    if df.empty:
        raise ValueError("Received empty dataframe")

    # --- 1. CLEANING ---
    df_clean = df.reset_index(drop=True)
    df_clean.columns = [str(c).lower().strip().replace(" ", "_") for c in df_clean.columns]

    # Safety check
    required_cols = {'date', 'symbol', 'adjclose',}
    if not required_cols.issubset(df_clean.columns):
        return None, {"is_empty": True, "error": "Missing required columns"}

    # Convert + clean
    df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce')
    if df_clean['date'].dt.tz is None:
        df_clean['date'] = df_clean['date'].dt.tz_localize('UTC')
    else:
        df_clean['date'] = df_clean['date'].dt.tz_convert('UTC')
    df_clean = df_clean.dropna(subset=['date', 'adjclose'])

    if df_clean.empty:
        return df_clean, {"is_empty": True}
    
    #Max and Min date
    
    min_date = df_clean['date'].min()
    max_date = df_clean['date'].max()

    # --- 4. MINIMAL RESULTS ---
    results = {
        "is_empty": False,
        "unique_tickers": int(df_clean['symbol'].nunique()),
        "date_range": f"{min_date.date()} to {max_date.date()}"
    }

    return df_clean, results

def audit_raw_data(df: pd.DataFrame, min_days_threshold: int = 55):
    """
    Audits stock data quality.

    - Counts trading days per ticker
    - Calculates coverage %
    - Flags tickers below threshold
    - Builds a concise results dictionary

    Returns:
    - DataFrame with audit flags
    - Results dictionary
    """

    # --- 0. EARLY EXIT ---
    if df is None:
        raise ValueError ("No data provided")
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f" Not a pandas dataframe. Got {type(df).__name__}")
    if df.empty:
        raise ValueError("Received an empty dataframe")

    df_audit = df.copy()

    # --- 1. BASIC CHECKS ---
    required_cols = {'date', 'symbol'}
    if not required_cols.issubset(df_audit.columns):
        return None, {"is_empty": True, "error": "Missing required columns"}

    # Ensure datetime
    df_audit['date'] = pd.to_datetime(df_audit['date'], errors='coerce')
    df_audit = df_audit.dropna(subset=['date'])

    if df_audit.empty:
        return df_audit, {"is_empty": True}

    # --- 2. DATE RANGE + EXPECTED DAYS ---
    min_date = df_audit['date'].min()
    max_date = df_audit['date'].max()
    expected_days = count_trading_days(min_date, max_date)

    # --- 3. COUNT ACTUAL DAYS PER TICKER ---
    ticker_counts = (
        df_audit.groupby('symbol')['date']
        .nunique()
        .reset_index(name='actual_days')
    )

    # --- 4. COVERAGE + FLAGS ---
    ticker_counts['coverage_pct'] = (
        ticker_counts['actual_days'] / expected_days * 100
    ).round(2)

    ticker_counts['is_flagged'] = (
        ticker_counts['actual_days'] < min_days_threshold
    )

    # --- 5. MERGE BACK TO DATA ---
    df_audit = df_audit.merge(
        ticker_counts,
        on='symbol',
        how='left'
    )

    # --- 6. BUILD RESULTS (FOCUSED) ---
    results = {
        "is_empty": False,
        "unique_tickers": int(ticker_counts['symbol'].nunique()),
        "expected_trading_days": int(expected_days),
        "date_range": f"{min_date.date()} to {max_date.date()}",
        "avg_coverage_pct": float(ticker_counts['coverage_pct'].mean()),
        "flagged_tickers_count": int(ticker_counts['is_flagged'].sum())
    }

    return df_audit, results

def validate_data_out(df):

    #confirm if the dataframe is none
    if df is None:
        raise ValueError(f"No values present in a dataframe for the validate_data_out function")

    #Confirm if it is a pandas dataframe
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Input is not a pandas dataframe. Got {type(df).__name__}")
    df.columns=[col.lower().replace(" ", "_") for col in df.columns]
    
    
    #confirm if there are no values in the dataframe
    if df.empty:
        raise ValueError(f" The dataframe has no values. It's empty")
    
    #confirm the minimum number of rows to be 6830
    if df.shape[0]<6830:
        raise ValueError(f" The dataframe has less than 6830 rows which rep the 110 tickers. got: {df.shape[0]}")
    
    #confirm the required columns
    required_col= ['symbol', 'date','adjclose','volume', 'coverage_pct', 'is_flagged', 'actual_days']
    missing_col=[]
    for col in required_col:
        if col not in df.columns:
            missing_col.append(col)

    if missing_col:
        raise ValueError(f" missing columns are {missing_col}")
    
    logger.info(f"VALIDATION OF HISTORICAL DATA COMPLETE")
    return df
     

if __name__ == "__main__":
    from config.logging_config import setup_logging
    from etl_pipeline.src.extract._clean_nasdaq_list import (
        validateInData, extract_columns, normalize_names,
        build_master_list, match_and_categorize,
        get_top_300, validate_top_300, pre_validate_with_yahoo
    )
    from etl_pipeline.src.extract._download_nasdaq_list import load_nasdaq_data

    setup_logging()
    logger.info("Starting to collect Historical Prices...")

    # Extract + prep
    df = load_nasdaq_data()
    df = validateInData(df)
    df = extract_columns(df)
    df = normalize_names(df)

    master = build_master_list(df)
    df = match_and_categorize(df, master)

    top_300 = get_top_300(df)
    top_300 = validate_top_300(top_300)

    # Fetch + process
    tickers = validate_tickers(top_300)
    raw_data = fetch_raw_data(tickers)

    clean_df, clean_results = clean_and_validate(raw_data)
    audited_df, audit_results = audit_raw_data(clean_df)
    historical_df=validate_data_out(audited_df)

    # Log results (not full dataframes)
    logger.info(clean_results)
    logger.info(audited_df.info())
    logger.info(historical_df)

    