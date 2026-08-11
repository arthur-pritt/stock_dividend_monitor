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
    - Filteres the watchllst_status column to have SKYROCKET & DROP column
    - Returns  ONLY the whole dataframe associated with SKYROCKET & DROP records."""

    # Confirm first if watchlist_status exist. If not raise an error/fail loudly

    if "watchlist_status" not in watchlist_df.columns:
        raise ValueError(f"The dataframe is missing a watchlist_status column")

    logger.info(f"Watchlist_column exist in the dataframe. PROCEED")

    #Filter unknown categorees in the dataset isin() method and  flag data quality issue with logger.warning

    allowed_statuses=['SKYROCKET', 'NORMAL', 'DROP']
    alert_statuses=['SKYROCKET','DROP']

    unknown_categories_df= watchlist_df[~watchlist_df['watchlist_status'].isin(allowed_statuses)].copy()
    

    alert_df=watchlist_df[watchlist_df['watchlist_status'].isin(alert_statuses)].copy()
    logger.info(f" Alert Status has {alert_df.shape[0]} record(s)")

    if unknown_categories_df.empty:
        logger.info(f"No unknown categorees in the dataset. PROCEED")
        
    else:
        logger.warning(f"\n=== Unknown categories exist===")
        logger.warning(f"Unknown categories has {unknown_categories_df.shape[0]} record(s)")
        logger.warning(f"{unknown_categories_df}")

    logger.info(f"SKYROCKET & DROP status filtered SUCCESSFULLY")

    #Sort by pct_change in descending order.
    alert_df=alert_df.sort_values(
        by=['pct_change'],
        ascending=False,
        ignore_index=True
    )

    logger.info(f"\nSKYROCKET & DROP dataset sorted SUCCESSFULLY")
    return alert_df

if __name__ == "__main__":
    logger.info(f"Starting the filtering process...")
    column_name = get_watchlist_status()
    filtered_data=filtered_watchlist_df(column_name)
    
