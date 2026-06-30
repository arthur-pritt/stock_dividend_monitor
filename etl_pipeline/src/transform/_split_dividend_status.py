import pathlib
import pandas as pd 
from pathlib import Path 

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

def validating_classified_stock(classified_csv: pathlib.Path)-> pd.DataFrame:
    """
    Validating of the classified_csv file structural integrity, loads it,
    and passing it to downstream dataframe validation."""


    if isinstance(classified_csv, pd.DataFrame):
        raise TypeError(
            "Expected a pathlib.Path object, but received a pandas dataframe."
            "Check the pipeline order"
        )


    #Checking file existence
    if not classified_csv.exists():
        raise FileNotFoundError(f" The file at {classified_csv} could not be found")
    
    #Confirming file extension
    if classified_csv.suffix.lower() != '.csv':
        raise ValueError(f"Unsupported file format '{classified_csv.suffix}'. Expected a .csv file.")
    
    #Checking if the file is empty
    if classified_csv.stat().st_size == 0:
        raise ValueError(f" CSV file is empty.") 
    
    #Can Pandas read it
    try:
        df=pd.read_csv(classified_csv)
    except pd.errors.EmptyDataError:
        raise ValueError(f" The CSV file at {classified_csv} is completely empty(no headers or data founds)")
    except pd.errors.ParserError as err:
        logger.error(f"Failed to parse CSv file. It may be structurally malformed.")
        raise ValueError(f" Malformatted CSV data at {classified_csv}: {err}") from err
    logger.info(f"File {classified_csv.name} parsed successfully. VALIDATION COMPLETE")

    return df

def split_dividend_status(classified_csv: pathlib.Path)-> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reads classification file path,
    Splits it into two dataframes,
    returns a tuple."""

    # Light validation (Test: None, data type, and empty data type)

    # Checking nulls on dividen-status column.
    # if null > 10%, pipeline stops, and raises a ValueError.
    # if null < 10%, pipeline continues, logs a warning, and writes bad to corrupted_data.csv

    #Splits into two dataframes (df_dividends and df_non_dividends).

    # Validating that data is neither created nor destroyed during the operation.
    # Output validation contract must confirm the equation of no data loss/creation

    raise NotImplementedError("Skeleon validated. Logic pending.")

if __name__ == "__main__":
    try:
        logger.info("=====Starting to split dividend_status column")
        csv_path = pathlib.Path(CLASSIFICATION_FILEPATH)
        validated_status_csv = validating_classified_stock(csv_path)
        #two_dataframes = split_dividend_status(validated_status_csv)
        print(validated_status_csv)

    except Exception as e:
        logger.error(f"Splitting FAILED: {str(e)}")



