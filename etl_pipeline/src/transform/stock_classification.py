import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv


from config.logging_config import(
    get_logger,
    setup_logging
)
from config.settings import (
    PROCESSED_SUBDIR,
    CLASSIFICATION_FILEPATH
)

from etl_pipeline.src.transform.staging import get_stock_table


PROCESSED_SUBDIR.mkdir(parents=True, exist_ok=True)
logger = get_logger(__name__)
setup_logging()
load_dotenv()


def validating_stock_data(staging_df):
    """
    Validating of the complete stock table data.
    """

    #Check/confirm if the data is None
    if staging_df is None:
        raise ValueError(" No data provided")
    
    #Check/confirm the data type is pandas
    if not isinstance(staging_df, pd.DataFrame):
        raise TypeError(f"Expected a pandas dataframe but got {type(staging_df).__name__}")
    
    #Check/confirm if the data type is empty
    if staging_df.empty:
        raise ValueError("Got an empty pandas dataframe")
    
    #Confirm/check the minimum number of rows
    n_rows =len(staging_df)

    if n_rows < 110:
        raise ValueError(f" Dataframe has only {n_rows} rows. Minimum required is 110 rows."
                         f"Received {n_rows} rows.")
    elif 110 <= n_rows <=300:
        logger.warning(f" DataFrame has {n_rows} rows. This is below the ideal range of 300 to 600"
                       f" Pipeline continues but results are limited.")
    else:
        pass 

    #confirm the required columns
    required_cols=['name', 'ticker', 'market_cap', 'dividend_per_share','adj_close','earnings_pershare', 'date']
    missing_col=[]

    for col in required_cols:
        if col not in staging_df:
            missing_col.append(col)
    
    if missing_col:
        raise ValueError(f" The data is missing key columns{required_cols}")
    
    return staging_df

def classify_stock_table(validated_stock_table):
    """
    A function that segments the companies that pay dividend and those that don't pay_dividend and returns a new column called dividend_status with
    values such as 'pays_dividend' and 'no_dividend'
    """

    #Validation

    if validated_stock_table is None:
        raise ValueError(f" No data provided.")
    
    if not isinstance(validated_stock_table, pd.DataFrame):
        raise TypeError(f" Expected a pandas dataframe, but got {type(validated_stock_table).__name__}")
    
    if validated_stock_table.empty:
        raise ValueError(f" The dataframe is empty.")

    #Segmenting tickers that pay dividend and non paying dividends
    validated_stock_table['dividend_status']= np.where(
        validated_stock_table['dividend_per_share'] < 0,
        "invalid_dividend",
        np.where(
            validated_stock_table['dividend_per_share']== 0,
            "no_dividend_payer",
            "dividend_payer"
        ) 
    )

    return validated_stock_table

def validated_segmented_tickers(segmented_tickers_df):
    """
    Validating outgoing data to the next phase.
    """

    #confirm if it is None
    if segmented_tickers_df is None:
        raise ValueError("No data to validate")
    
    #Confirm the pandas datatype
    if not isinstance(segmented_tickers_df, pd.DataFrame):
        raise TypeError(f" Expected pandas dataframe got {type(segmented_tickers_df).__name__}")
    
    #confirm the dataframe is empty
    if segmented_tickers_df.empty:
        raise ValueError(f" The dataframe is empty.")
    
    if segmented_tickers_df.shape[0]<300:
        raise ValueError(f"The dataframe has less than 150 rows which represent less than 100 tickers")
    
    #checking the required columns
    required_col = [
        'ticker',
        'earnings_pershare',
        'dividend_per_share',
        'adj_close',
        'dividend_status',
        'date']
    missing_col=[]

    for col in required_col:
        if col not in segmented_tickers_df.columns:
            missing_col.append(col)
    
    if missing_col:
        raise ValueError(f" Missing columns are {missing_col}")
    
    logger.info(f" VALIDATION OF CLASSIFICATION TABLE COMPLETE")

    return segmented_tickers_df

def get_classified_ticker_df():
    """
    Checks if the stock table contains fresh data and orchestrates the
    entire stock classification file."""
    
    logger.info("Starting to Fetch fresh segmented ticker data..")

    if CLASSIFICATION_FILEPATH.is_file(): #Checks if the classification file table exists
        last_modified = datetime.fromtimestamp(os.path.getatime(CLASSIFICATION_FILEPATH))
        if datetime.now() - last_modified <= timedelta(days=1): #File-based frehsness checking
            logger.info(f" File Found, loading fresh segmented stock data from the disk...")
            return pd.read_csv(
                CLASSIFICATION_FILEPATH,
                dtype= {
                    'name': str,
                    'ticker': str,
                    'market_cap': float,
                    'adj_close': float,
                    'dividend_per_share': float,
                    'earnings_pershare': float,
                    'dividend_status':str
                }
            )
    #Fetch fresh
    ticker_table = get_stock_table()
    ticker_identity = validating_stock_data(ticker_table)
    segmented_tickers= classify_stock_table(ticker_identity)
    validated_tickers = validated_segmented_tickers(segmented_tickers)

    #Saving the fresh segmented ticker data
    fresh_segmented_data = validated_tickers
    fresh_segmented_data.to_csv(
        CLASSIFICATION_FILEPATH,
        index=False,
        float_format="%.2f",
        na_rep="NA",
        encoding="utf-8"
    )

    logger.info("Pipeline Executed Successfuly. FRESJ segmented ticker data READY")
    return fresh_segmented_data


if __name__ == "__main__":
    try:
        logger.info("====Starting to classify the tickers===")
        segmented_tickers= get_classified_ticker_df()
        print("\n==============PIPELINE SUCCESS===")
        print(segmented_tickers)
        

    except Exception as e:
        logger.error(f"Classfication FAILED: {str(e)}")
    

