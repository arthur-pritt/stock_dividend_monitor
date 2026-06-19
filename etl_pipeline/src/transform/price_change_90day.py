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


def price_change_calculation(classified_data, historical_data):
    #historical_data,
    """
    Calculate how much a stock has moved in the last 90 days.
    """

    #Take the most recent row per ticker from classified data from the data.
    classified_data = (
        classified_data
        .sort_values("date")
        .drop_duplicates(subset='ticker', keep="last")
    )

    logger.info(classified_df)

    # Take the oldest row per ticker from backfill.py

    historical_data = (
        historical_data
        .sort_values("date")
        .drop_duplicates(subset='ticker', keep='last')
    )

    # Renaming columbs in both files

    classified_data  = classified_data.rename(columns={
        "adj_close" : "current_adjclose"
    })

    historical_data = historical_data.rename(columns={
        "adjclose" : "historical_adjclose"
    })

    #dropping columns in the backfill.py file
    historical_data = historical_data.drop(columns=[
        'open','high','low','close','volume',
        'coverage_pct','is_flagged'
    ])

    dfs=[
        classified_data,
        historical_data
    ]

    #Prehandle the schemas and data types before reductions begins
    #Ensure the join key is strictly the same across all the dataset since it is a string to prevent bugs

    def sanitize_dataframe(df):
        if 'ticker' in df.columns:
            df['ticker'] = df['ticker'].astype(str)

        return df 
    
    #Apply sanitization to all dataframes
    price_change_df= [sanitize_dataframe(df) for df in dfs]

    try:
        price_change_table = reduce(
            lambda left, right: pd.merge(
                left,
                right, 
                on= 'ticker',
                how='left'
            ),
            price_change_df
        )#Clean up any duplicated columns generated during the loop
        price_change_table= price_change_table.loc[:, ~price_change_table.columns.str.endswith('_dup')]

    except TypeError:
        logger.error("Error: The dataframe list was empty")
        price_change_table= pd.DataFrame() #Fallback

    return price_change_table


if __name__ == "__main__":
    historical_df = get_historical_data()
    classified_df = get_classified_ticker_df()
    dataframes = {
        "historical_df":historical_df,
        "classified_df":classified_df
    }

    for name, df in dataframes.items():
        validating_two_dataframes(df,name)

    merged_table = price_change_calculation(classified_df,historical_df)
    print(merged_table)



   