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
    CACHED_DIVIDEND_FILEPATH,
    CACHED_NON_DIVIDEND_FILEPATH
)

from etl_pipeline.src.transform.stock_classification import get_classified_ticker_df

PROCESSED_SUBDIR.mkdir(parents=True, exist_ok=True)
setup_logging()
logger = get_logger(__name__)

def validating_classified_stock(classified_csv: pathlib.Path)-> pathlib.Path:
    """
    Validating of the classified_csv file:
    checks existence, extension, emptiness, and that pandas can parse it and 
    then hands back the path."""

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

    return classified_csv

def split_dividend_status(classified_csv: pathlib.Path)-> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reads classification file path,
    Splits it into two dataframes,
    returns a tuple."""

    # Light validation (Test: None, data type, and empty data type)
    if not isinstance(classified_csv, pathlib.Path):
        raise TypeError(
            f"Expected a pathlib.Path object, but received a {type(classified_csv)}."
            f"Check the pipeline order."
        )
    
    if not classified_csv.exists():
        raise FileNotFoundError(f" The file at {classified_csv} could not be found.")
    
    if classified_csv.suffix.lower() != ".csv":
        raise ValueError(f"Unsupported file format '{classified_csv}'. Expected a CSV file")

    if classified_csv.stat().st_size == 0:
        raise ValueError(f" CSV file is empty.")

    # Checking nulls on dividen_status column.
    # if null > 10%, pipeline stops, and raises a ValueError.
    # if null < 10%, pipeline continues, logs a warning, and writes bad to corrupted_data.csv
    
    try:
        df=pd.read_csv(classified_csv)
        total_input_rows=len(df)
        logger.info(f"Total Expected rows: {total_input_rows}")
    
    except Exception as e:
        logger.error(f"Reading of Pandas CSV FAILED: {str(e)}")
        raise


        
    if df['dividend_status'].isna().mean() *100 >10.0:
        raise ValueError(f" PIPELINE STOPS: The null values exceed more than 10% of the entire dataframe.")
        
    else:
        logger.info(f" PIPELINE PROCEEDS......")

        #Create the null musk
        null_musk = df['dividend_status'].isna()

        #using the mask to isolate the actual bad rows
        df_corrupt= df[null_musk]

        if len(df_corrupt) >0:
            logger.warning(f"{len(df_corrupt)} tickers with null values written to corrupted_data.csv")
            df_corrupt.to_csv(
                classified_csv.parent / "corrupted_data.csv",
                index=False,
                encoding='utf-8')
    
    #Keeping only clean data in memory for the rest function
    df=df[~null_musk]

    #Removing bad
    allowed=['dividend_payer', 'no_dividend_payer']
    valid_status_mask=df["dividend_status"].isin(allowed)
    invalid_status_df=df[~valid_status_mask]
    if len(invalid_status_df) > 0:
        logger.warning(f"{len(invalid_status_df)} tickers with wrong status write to wrong_status.csv")
        invalid_status_df.to_csv(
            classified_csv.parent / "wrong_status.csv",
            index=False,
            encoding='utf-8')

    #Splits into two dataframes (df_dividends and df_non_dividends).
    df_dividend =df[df['dividend_status']=='dividend_payer']
    df_non_dividend = df[df['dividend_status']=='no_dividend_payer']

    

    # Validating that data is neither created nor destroyed during the operation.
    # Output validation contract must confirm the equation of no data loss/creation

    total_output_rows= len(df_dividend) + len(df_non_dividend) + len(df_corrupt) + len(invalid_status_df)

    if total_input_rows != total_output_rows:
        raise RuntimeError(
            f"Data corruption detected! Input rows ({total_input_rows})"
            f"do not match total output rows ({total_output_rows})"
        )
    
    logger.info(f"Splitting of dividend_status column COMPLETE")
    return df_dividend, df_non_dividend
def get_dividend_type(classified_csv:pathlib.Path)-> tuple[pd.DataFrame,pd.DataFrame]:

    """
    Fetches the dividend status  and orchestrates the entire file to be used
    by other function.
    """
    # Splitting the dividend column into twos( dividend companies and non_dividend_companies)
    validated_status_csv= validating_classified_stock(classified_csv)
    df_dividend, df_non_dividend=split_dividend_status(validated_status_csv)

    #Saving the dividend and non_dividend files to CSV
    df_dividend.to_csv(
        CACHED_DIVIDEND_FILEPATH,
        index= False,
        float_format= "%.2f",
        na_rep="NA",
        encoding="utf-8"
        
    )

    df_non_dividend.to_csv(
        CACHED_NON_DIVIDEND_FILEPATH,
        index= False,
        float_format= "%.2f",
        na_rep="NA",
        encoding="utf-8"
    )
    
    logger.info("Pipeline Executed the split successfully. Two dataframe saved: " \
    "DIVIDEND PAYING COMPANIES & NON DIVIDEND PAYING COMPANIES")
    return df_dividend, df_non_dividend

if __name__ == "__main__":
    try:
        logger.info("=====Starting to split dividend_status column")
        csv_path=pathlib.Path(CLASSIFICATION_FILEPATH)
        dividend_companies, non_dividend_companies=get_dividend_type(csv_path)
        print(dividend_companies)

    except Exception as e:
        logger.error(f"Splitting FAILED: {str(e)}")



