import pandas as pd
from functools import reduce 
from edgar import set_identity
import os 
from dotenv import load_dotenv
import pandera.pandas as pa


from config.logging_config import (
    get_logger,
    setup_logging
)

from etl_pipeline.src.extract._clean_nasdaq_list import get_nasdaq_list
from etl_pipeline.src.extract._fetch_stock_price import get_price_data
from etl_pipeline.src.extract._fetch_dividend_data import get_dividend_data
from etl_pipeline.src.extract._fetch_earnings_data import get_earning_data
from etl_pipeline.src.schema.ticker_schemas import CURRENT_PRICE_FILE_SCHEMA
from config.settings import DATA_COLS


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
        data_list
):
    """
    Merging four data sources(fetch_stock_price, dividend_data,nasdaq_list, earning_data)
    into a unified master table. The most recent quarter's dividend is attached to the daily price
    """
    

    dfs=[
        prices_table,
        dividend_table,
        earning_table,
        data_list
    ]

    merged_df= reduce(
        lambda left, right:
        left.merge(right, on="ticker"),
        dfs
    )

    return merged_df

if __name__ == "__main__":
    setup_logging()
    set_identity(os.environ.get("EDGAR_IDENTITY"))

    try:
        logger.info("====Starting to merge all four files===")

        final_list= get_nasdaq_list()
        validated_staging_data = validate_data_list(final_list)
        print(validated_staging_data)
        #data_list = get_nasdaq_list()
        #prices_data=get_price_data(data_list)
        #dividend_data = get_dividend_data(data_list)
        #earning_data = get_earning_data(data_list)

        #ticker_table=unified_ticker_table(data_list,prices_data,dividend_data,earning_data)
        #print(ticker_table.head())
    except Exception as e:
        logger.error(f"Merging FAILED: {str(e)}")




