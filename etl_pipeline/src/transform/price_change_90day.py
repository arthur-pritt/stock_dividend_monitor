import pandas as pd
from functools import reduce
from edgar import set_identity


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


def price_change_calculation(
        df,
        name
)-> pd.DataFrame:
    """
    Calculate how much a stock has moved in the last 90 days and add a watchlist_status column.
    """

    #Light validation of the two dataframes(df isNone, df isinstance, dfempty).

    #For classified_data, grab the row with the maximum date max(date) per ticker

    #For historical data, grab the row with the minimum date min(date) per ticker

    #Column renaming in both classified_data & historical_data
    #For classified_data, adj_close to current_adjclose, date to current_date
    #For historical_data, adjclose to historical_adjclose, date to historical_date

    #Dropping of columns in the historical_date such as open, high, low, close, volume, actual_days,coverage_pct, and is flagged

    #DATA QUALITY GATE:Zero values/10% error budget. If the percentage of zero values exists in the historical_data and classified_data and is more than 10
    #%, the pipeline stops. If less than that, the pipeline logs a warning and saves the bad data to zero_tickers.csv.
    #Bad rows dropped.

    #DATA QUALITY GATE:Identify which tickers exist in one slice but not the other. Export those discrepancies to failed_joined_tickers.csv
    #Perform inner join between historical_data and classified_data.some entries here may be tickers already excluded upstream for zero-price — check zero_tickers.csv first

    #DATA QUALITY GATE: Calculate the delta between current_date and historical_date. Identify rows outside your acceptable calendar day threshold, log them to dropped_tickers.csv, and filter them out.
    #if the difference between min(date) and max(date) is less than 85 or 95 dats. And create actual_days column

    #Math & Logic Engine: Safely compute pct_change (since zeros are gone) and apply your conditional labeling for the watchlist_status

    #Output: ticker, marketcap,dividend_status,historical_date, historical_adjclose, current_date, current_adjclose, actual_days
    #dividend_per_share, pct_change, watchlist_status

    raise NotImplementedError("Skeleton validated. Logic pending....")


if __name__ == "__main__":
    historical_df = get_historical_data()
    classified_df = get_classified_ticker_df()
    dataframes = {
        "historical_df":historical_df,
        "classified_df":classified_df
    }

    for name, df in dataframes.items():
        validating_two_dataframes(df,name)

    print(historical_df.head(10))
    print("Classified")
    print(classified_df.head(10))

    merged_table = price_change_calculation(classified_df,historical_df)
    print(merged_table)



   