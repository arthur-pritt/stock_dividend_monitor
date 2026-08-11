import pandas as pd

#importing the function
from etl_pipeline.src.transform.price_change_90day import get_watchlist_status
from etl_pipeline.src.transform.dividend_yield_calculator import get_dividend_calculation

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
    if unknown_categories_df.empty:
        logger.info(f"No unknown categories in the dataset. PROCEED")

    else:
        logger.warning(f"\n=== Unknown categories exist===")
        logger.warning(f"Unknown categories has {unknown_categories_df.shape[0]} record(s)")
        logger.warning(f"{unknown_categories_df}")

    alert_df=watchlist_df[watchlist_df['watchlist_status'].isin(alert_statuses)].copy()
    logger.info(f" Alert Status has {alert_df.shape[0]} record(s)")
    logger.info(f"SKYROCKET & DROP status filtered SUCCESSFULLY")

    #Sort by pct_change in descending order.
    alert_df=alert_df.sort_values(
        by=['pct_change'],
        ascending=False,
        ignore_index=True
    )

    logger.info(f"\nSKYROCKET & DROP dataset sorted SUCCESSFULLY")
    return alert_df

def filtered_action_signal(div_gain_df:pd.DataFrame)->pd.DataFrame:
    """
    - Takes the dataframe returned by dividend gain calculation.
    - Filters the action_signal column to have all the THREE SELL signals excluding HOLD.
    - Returns ONLY the dataframe with THREE SELL signals.
    """

    # Confirm that sell signal column exists in the dataframe and fail loudly if it doesn't exist.
    if "action_signal" not in div_gain_df.columns:
        raise ValueError(f"ACTION_SIGNAL column does not exist in the dataframe")

    logger.info(f"ACTION_SIGNAL column exists in the dataframe. PROCEED")
    # Flag "Unknown_status" in the action_signal column(it is a column that has 4 status).
    #Filter only for three sell status excluding HOLD in this case) using isin() method

    allowed_status=['SELL_PARTIAL_STAGE_3_20_PCT',
                    'SELL_PARTIAL_STAGE_2_30_PCT',
                    'SELL_PARTIAL_STAGE_1_30_PCT',
                    'HOLD']

    allowed_sell_signals=[
        'SELL_PARTIAL_STAGE_3_20_PCT',
        'SELL_PARTIAL_STAGE_2_30_PCT',
        'SELL_PARTIAL_STAGE_1_30_PCT'
    ]

    unknown_sell_categories_df=div_gain_df[~div_gain_df['action_signal'].isin(allowed_status)].copy()
    if unknown_sell_categories_df.empty:
        logger.info(f"No unknown action sell categories exists in the dataframe. PROCEED")

    else:
        logger.warning(f"\n===Unknown status categories")
        logger.warning(f"Unknown sell categories has {unknown_sell_categories_df.shape[0]} record(s)")
        logger.warning(f"{unknown_sell_categories_df}")

    action_signal_df=div_gain_df[div_gain_df['action_signal'].isin(allowed_sell_signals)].copy()
    logger.info(f"Action_signal dataframe has {action_signal_df.shape[0]} record(s)")
    logger.info(f"All the THREE SELL SIGNALS filtered Successfully. PROCEED")

    #Sort the action signal dataframe by pct_change DESC and grouping by categories

    logger.info(f"SELL signal status ranked according to Priority with SELL_PARTIAL_STAGE_3_20_PCT taking First Priority")
    priority_map= {
        'SELL_PARTIAL_STAGE_3_20_PCT' : 1,
        'SELL_PARTIAL_STAGE_2_30_PCT' : 2,
        'SELL_PARTIAL_STAGE_1_30_PCT' : 3
    }
    
    
    action_signal_df['alert_priority']=(
        action_signal_df['action_signal']
        .map(priority_map)
    )

    action_signal_df=action_signal_df.sort_values(
        by=['alert_priority','pct_change'],
        ascending=[True,False],
        ignore_index= True
    )

    logger.info(f"SELL SIGNAL STATUS contains {action_signal_df.shape[0]} record(s)")
    logger.info(f"\nSELL SIGNAL STATUS filtered and sorted successfully. PROCEED")

    return action_signal_df

if __name__ == "__main__":
    logger.info(f"Starting the filtering process...")
    column_name = get_watchlist_status()
    filtered_data=filtered_watchlist_df(column_name)
    div_signal=get_dividend_calculation()
    sell_signal=filtered_action_signal(div_signal)
    
    
