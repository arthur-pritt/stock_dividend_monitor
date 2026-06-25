import pathlib
import pandas as pd 

from config.logging_config import (
    get_logger,
    setup_logging
)

from config.settings import (
    PROCESSED_SUBDIR,
    CLASSIFICATION_FILEPATH,
    CORRUPTED_DATA_FILEPATH
)

from etl_pipeline.src.transform.stock_classification import get_classified_ticker_df

PROCESSED_SUBDIR.mkdir(parents=True, exist_ok=True)
setup_logging()
logger = get_logger(__name__)

def validating_classified_stock(classified_csv: pathlib.Path)-> None:
    """
    Validating of the classified_csv file."""
    pass 

def split_dividend_status(classified_csv: pathlib.Path)-> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reads classification file path,
    Splits it into two dataframes,
    returns a tuple."""

    # Light validation (Test: None, data type, and empty data type)

    # Checking nulls on dividen-status column.
    # if null > 10%, pipeline stops, and raises a ValueError.
    # if null < 10%, pipeline continues, logs a warning, and writes bad to corrupted_data.csv

    #Splits into two ddataframes (df_dividends and df_non_dividends).

    # Validating that data is neither created nor destroyed during the operation.
    # Output validation contract must confirm the equation of no data loss/creation

    raise NotImplementedError("Skeleon validated. Logic pending.")

if __name__ == "__main__":
    try:
        logger.info("=====Starting to split dividend_status column")
        dividend_path = get_classified_ticker_df()
        split_dividend = validating_classified_stock(dividend_path)
        two_dataframes = split_dividend_status(split_dividend)

    except Exception as e:
        logger.error(f"Splitting FAILED: {str(e)}")



