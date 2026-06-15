import pandas as pd
from functools import reduce 
from edgar import set_identity
import os 
from dotenv import load_dotenv
import pandera.pandas as pa
from datetime import datetime, timedelta


from config.logging_config import (
    get_logger,
    setup_logging
)

from etl_pipeline.src.extract._clean_nasdaq_list import get_nasdaq_list
from etl_pipeline.src.extract._fetch_stock_price import get_price_data
from etl_pipeline.src.extract._fetch_dividend_data import get_dividend_data
from etl_pipeline.src.extract._fetch_earnings_data import get_earning_data
from etl_pipeline.src.schema.ticker_schemas import CURRENT_PRICE_FILE_SCHEMA
from config.settings import (
    DATA_COLS,
    STAGING_SUBDIR,
    STAGING_FILEPATH
)

STAGING_SUBDIR.mkdir(parents=True, exist_ok=True)


logger =get_logger(__name__)
setup_logging()
load_dotenv()

def validate_data_list(df):
    """
    Validating the nasdaq_list of 300 tickers.
    """

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

def unified_ticker_table(
        prices_table,
        dividend_table,
        earning_table,
        data_list,
):
    """
    Merging four data sources(fetch_stock_price, dividend_data,nasdaq_list, earning_data)
    into a unified master table. The most recent quarter's dividend is attached to the daily price
    """

    #Simulating four dataframes

    prices_table = prices_table.drop(columns=['year','month'])
    dividend_table = dividend_table.drop(columns=['cik'])
    earning_table = earning_table.drop(columns=['cik','quarter','year'])

    dfs=[
        data_list,
        prices_table,
        dividend_table,
        earning_table
    ]

    #Prehandle the schemas and data types *before* reduction begins
    #Ensuring that the join key is strictly the same across all the dataset since it is a string to prevent bugs
    def sanitize_dataframe(df):
        if 'ticker' in df.columns:
            df['ticker'] = df['ticker'].astype(str)
        return df 
    
    # Apply sanitization to all dataframes
    clean_dfs = [sanitize_dataframe(df) for df in dfs]

    try:
        complete_stock_table = reduce(
            lambda left, right: pd.merge( #reduce and pd.merge used to have a maximum granular control over column-based joins.
                left,
                right,
                on='ticker',
                how='left'
            ),
           clean_dfs 
        )
        # Clean up any duplicated columns generated during the loop
        complete_stock_table= complete_stock_table.loc[:, ~complete_stock_table.columns.str.endswith('_dup')]
    except TypeError:
        logger.error("ERROR: The dataframe list was empty")
        complete_stock_table= pd.DataFrame() #Fallback state

    return complete_stock_table

def cleaning_stock_table(df):
    """"
    Investigates and cleans the data.
    """

    #Changing the values of column to title case
    df['name']=df['name'].str.title()

    report = []
    #Doing an audit to find missing values, duplicates, confirming datatype


    for col in df.columns:
        missing = df[col].isna().sum()
        duplicates = df[col].duplicated().sum()

        recommendations = []
        if missing:
            recommendations.append("Handle missing values")

        if duplicates:
            recommendations.append("Check for duplicates")

        if df[col].dtype == "object":
            recommendations.append("Check duplicates")

        if df[col].dtype == "Object":
            recommendations.append("Check text standardization")

        report.append({
            "column": col,
            "missing_count":missing,
            "duplicate_count":duplicates,
            "action_needed":
                   "Yes" if recommendations else "NO",
            "recommendations":
            ", ".join(recommendations)}
        )
        final_report = pd.DataFrame(report)

    return df

def validating_stock_table(stock_df):
    """
    Validating and saving the output."""
    
    #Confirm it is None
    if stock_df is None:
        raise ValueError("No data to validate")
    
    #Confirm the data type 
    if not isinstance(stock_df, pd.DataFrame):
        raise TypeError(f" Expected a pandas dataframe got {type(stock_df).__name__}")
    
    #confirm the dataframe is empty
    if stock_df.empty:
        raise ValueError("The dataframe is empty")
    
    #confirm the number of rows
    if stock_df.shape[0]<300:
        raise ValueError(f"The dataframe has less than 300 rows which represent less than 100n tickers")
    
    #confirming the required columns
    required_col = ['ticker', 'earnings_pershare', 'adj_close', 'dividend_per_share']
    missing_col = []

    for col in required_col:
        if col not in stock_df.columns:
            missing_col.append(col)

    if missing_col:
        raise ValueError(f" Missing columns are {missing_col}")

    logger.info(f" VALIDATION OF STOCK TABLE COMPLETE")
    # Saving the staging results to CSV file
    return stock_df

def get_stock_table():
    """
    Checks if the stock table contains fresh data and orcherstars
    the entire staging file."""


    logger.info("Starting to Fetch fresh stock table.=====")


    if STAGING_FILEPATH.is_file(): #Check if the complete stock table file exists
        last_modified = datetime.fromtimestamp(os.path.getatime(STAGING_FILEPATH))
        if datetime.now()-last_modified <= timedelta(days=1): #File-based freshness checking
            logger.info(f"File Found, loading fresh stock data from the disk....")
            return pd.read_csv(
                STAGING_FILEPATH,
                dtype={
                    "ticker": str,
                    "adj_close": float,
                    "market_cap": float,
                    "dividend_per_share":float,
                    "earnings_pershare" : float
                }
            )
        
    # Gather everything
    final_list = get_nasdaq_list()
    staging_data = validate_data_list(final_list)
    prices_data = get_price_data(staging_data)
    dividend_data= get_dividend_data(staging_data)
    earning_data = get_earning_data(staging_data)
    ticker_table= unified_ticker_table(prices_data,dividend_data, earning_data, staging_data)
    clean_stock_table = cleaning_stock_table(ticker_table)
    saved_stock_data=validating_stock_table(clean_stock_table)

    #Saving the fresh stock table data.
    fresh_stock_data = saved_stock_data
    fresh_stock_data.to_csv(
        STAGING_FILEPATH,
        index= False,
        float_format= "%.2f",
        na_rep="NA",
        encoding="utf-8"
    )

    logger.info("Stock Table Executed Successfuly. FRESH Stock Table Data READY.")
    return fresh_stock_data
    

if __name__ == "__main__":
    set_identity(os.environ.get("EDGAR_IDENTITY"))

    try:
        logger.info("====Starting to merge all four files===")
        merged_files=get_stock_table()
        print("\n====PIPELINE SUCCESS===")
        print(merged_files)
    

    except Exception as e:
        logger.error(f"Merging FAILED: {str(e)}")




