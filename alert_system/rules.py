import pandas as pd

#importing the function
from etl_pipeline.src.transform.price_change_90day import get_watchlist_status

from config.logging_config import (
    setup_logging,
    get_logger
)

setup_logging()
logger= get_logger(__name__)

def filtered_watchlist_df(watchlist_df:pd.DataFrame)->pd.DataFrame :
    """
    - Takes the watchlist dataframe function.
    - Filteres the watchllst_status column to have SKYROCKET & DROP olcumn
    - Returns  ONLY the whole dataframe associated with SKYROCKET & DROP records."""

    # Confirm first if watchlist_status exist. If not raise an error/fail loudly

    if "watchlist_status" not in watchlist_df.columns:
        raise ValueError(f" {watchlist_df} is missing a watchlist_status column")

    logger.info(f"Watchlist_column exist in the dataframe. PROCEED")

    #Filret the status column using isin() method and  flag data quality issue with logger.warning

    filtred_categories= watchlist_df[watchlist_df['watchlist_status'].isin(['SKYROCKET','DROP'])]
    print(filtred_categories[0:50])


    return watchlist_df

if __name__ == "__main__":
    logger.info(f"Starting the filtering process...")
    column_name = get_watchlist_status()
    filtered_data=filtered_watchlist_df(column_name)
    
