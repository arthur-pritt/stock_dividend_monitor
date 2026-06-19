import pandas as pd


from config.logging_config import(
    setup_logging,
    get_logger
)

from etl_pipeline.src.extract._backfill import get_historical_data
from etl_pipeline.src.transform.stock_classification import get_classified_ticker_df



setup_logging()
logger= get_logger(__name__)

def validating_two_dataframes(df,
                              name,
                              required_columns=None,
                              expected_schema=None,):
    
    """
    Validating incoming inputs from both classified_df and historical_df.
    """

    #Basic validation

    if df is None:
        raise ValueError(f"{name}: dataframe is None")
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f" {name}: expected Dataframe, got {type(df).__name__}")
    
    if df.empty:
        raise ValueError(f" {name}: Dataframe is empty")
    
    # Required columns check

    if required_columns:
        missing_columns = (
            set(required_columns)-set(df.columns)
        )

        if missing_columns:
            raise ValueError(
                f"{name}: missing columns"
                f"{sorted(missing_columns)}"
            )
        
    #Schema Conformance

    if expected_schema:
        for column, expected_dtype in expected_schema.items():
            if column not in df.columns:
                continue 

            actual_dtype = str(df[column].dtype)

            if actual_dtype != expected_dtype:
                raise TypeError(
                    f"{name}: column '{column}' "
                    f"expected {expected_dtype},"
                    f"got {actual_dtype}"
                )
            
    logger.info(f" PASSED: All validation for both historical data and classification data.")
    return True 

if __name__ == "__main__":
    historical_df = get_historical_data()
    classified_df = get_classified_ticker_df()
    dataframes = {
        "historical_df":historical_df,
        "classified_df":classified_df
    }

    for name, df in dataframes.items():
        validating_two_dataframes(df,name)



   