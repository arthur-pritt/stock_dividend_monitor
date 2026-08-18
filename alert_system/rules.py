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
    logger.info(f"Watchlist_status has {alert_df.shape[0]} record(s)")
    logger.info(f"SKYROCKET & DROP status filtered SUCCESSFULLY")

    #Sort by pct_change in descending order.
    alert_df=alert_df.sort_values(
        by=['pct_change'],
        ascending=False,
        ignore_index=True
    )

    logger.info(f"\nSKYROCKET & DROP dataset sorted SUCCESSFULLY")
    print(alert_df[0:10])
    return alert_df

def filtered_action_signal(div_gain_df:pd.DataFrame)->pd.DataFrame:
    """
    - Takes the dataframe returned by dividend gain calculation.
    - Filters the action_signal column to have all the THREE SELL signals excluding HOLD.
    - Returns ONLY the dataframe with THREE SELL signals and adds a priority-ranking column
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

    #print(action_signal_df[0:10])

    return action_signal_df

def compute_ticker_transition(
        yesterday_df:pd.DataFrame,
        today_df:pd.DataFrame,
        status_column: str
        )->tuple[
            pd.DataFrame,
            pd.DataFrame, 
            pd.DataFrame, 
            pd.DataFrame
            ]:
    """
    -Takes yesterday's and today's filtered dataframe of the same report type( filtered_watchlist_df and filtered_action_df)
    -Returns  four different dataframe buckets describing what changed.
    """

    #Receives yesterday_df and today_df
    #merge on ticker(ticker matching rows)

    logger.info(f"Merging started: yesteday's and today's filtered dataframe of the same report type")

    merged_df= yesterday_df.merge(
        today_df,
        on=["ticker"],
        how="outer",
        indicator=True,
        validate="one_to_one",
        suffixes=("_yesterday","_today")
    )

    #Creation of 4 buckets:

    yesterday_column= status_column + '_yesterday'
    today_column= status_column + '_today'

    logger.info(f"Merging COMPLETE")
    logger.info(f"Creation of 4 buckets: Unchanged_status, Changed_status, Exited_tickers, New_tickers")
    #Unchanged_status-BOTH TICKERS STATUS REMAIN UNCHANGED
    unchanged_status=((merged_df['_merge']=="both") & (merged_df[yesterday_column] == merged_df[today_column]))
    unchanged_df=merged_df[unchanged_status]

    logger.info(f"Number of unchanged ticker status {unchanged_df.shape[0]}")


    #Changed Status- BOTH TICKERS STATUS PRESENT BUT POSITION CHANGES
    changed_status=((merged_df['_merge'] =="both") & (merged_df[yesterday_column] != merged_df[today_column]))
    changed_df=merged_df[changed_status]
    logger.info(f"Number of changed ticker status {changed_df.shape[0]}")

    
    #Dropped_tickers_status- Ticker Present Yesterday but today the position changed
    exited_tickers=merged_df['_merge'] =="left_only"
    exited_df=merged_df[exited_tickers]

    logger.info(f"Number of dropped/exited tickers {exited_df.shape[0]}")

    #New_tickers_status- New Ticker Present Today. New Position
    new_tickers=merged_df['_merge']=="right_only"
    new_df = merged_df[new_tickers]

    logger.info(f"Number of new tickers {new_df.shape[0]}")
    logger.info(f"\nCreation of four Dataframe:COMPLETE")

    return (unchanged_df,changed_df,exited_df,new_df)

def missing_tickers_report(exited_df:pd.DataFrame,watchlist_df:pd.DataFrame)->tuple[pd.DataFrame, pd.DataFrame]:
    """
    -Takes two dataframe exited_df and watchlist_df
    -Exited_df checks for membership in watchlist_df
    -Returns two dataframe as tuple"""

    logger.info(f"Checking missing tickers if present or genuinely missing from the watchlist universe")

    #Validate the ticker column in both exited_df & watchlist_df
    if ("ticker" not in exited_df.columns
        or 'ticker' not in watchlist_df.columns):

        raise ValueError(f"Ticker column does not exist either in exited_df or watchlist_df")

    logger.info(f"Ticker column present in both dataframe.")
    
    #Takes exited_df and checks ticker membership in watchlist_df with isin()method
    #Tickers present in watchlist_df are still_present_df
    #Tickers complete absent in watchlist_df are genuinely_missing_df

    membership_mask=exited_df['ticker'].isin(watchlist_df['ticker'])

    still_present_df=exited_df[membership_mask]
    genuinely_missing_df=exited_df[~membership_mask]
    logger.info(f"Producing two dataframes: still_present_df and genuinely_missing_df")
    logger.info(f"Tickers that are still present in the watchlist universe: {still_present_df.shape[0]}")
    logger.warning(f"Tickers that are absent in the watchlist universe: {genuinely_missing_df.shape[0]}")

    return(still_present_df,genuinely_missing_df)

def generate_watchlist_report(changed_df:pd.DataFrame, new_entrants_df:pd.DataFrame)->pd.DataFrame:
    """
    Takes changed_df and new_entrants_df as inputs and combines them to produce a pandas dataframe.
    """

    #Validation

    if (
        "ticker" not in changed_df.columns
        or "pct_change_today" not in changed_df.columns
        or "ticker" not in new_entrants_df.columns
        or "pct_change_today" not in new_entrants_df.columns
    ):
        raise ValueError(f"Either  changed_df or new_entrants_df is MISSING REQUIRED COLUMNS")

    #Combine changed_df and new_entrants_df
    combined_df=pd.concat([changed_df,
                           new_entrants_df],
                           axis=0,
                           ignore_index=True)

    #Sort by pct_change_today
    sorted_df=combined_df.sort_values(
        "pct_change_today",
        ascending=False,
        ignore_index=True
    )

    return  sorted_df

def generate_action_signal_report(changed_df:pd.DataFrame, new_entrants_df:pd.DataFrame)->pd.DataFrame:
    """
    Takes changed_df and new_entrants_df as in puts and combine to produce a report
    """

    #Validation of two essential columns

    if (
        "ticker" not in changed_df.columns
        or "pct_change_today" not in changed_df.columns
        or "ticker" not in new_entrants_df.columns
        or "pct_change_today" not in new_entrants_df.columns
    ):
        raise ValueError(f"Either changed_df or new_entrants df is MISSING THE REQUIRED COLUMNS")

    #combine changed_df and new_entrants_df
    combined_df=pd.concat([
        changed_df,
        new_entrants_df
    ],
    axis=0,
    ignore_index=True)

    #Sort by pct_change_today
    sorted_df= combined_df.sort_values(
        "pct_change_today",
        ascending=False,
        ignore_index=True
    )

    return sorted_df

    

if __name__ == "__main__":
    logger.info(f"Starting the filtering process...")
    watchlist_data = get_watchlist_status()
    filtered_watchlist=filtered_watchlist_df(watchlist_data)
    sell_signal_data=get_dividend_calculation()
    sell_signal=filtered_action_signal(sell_signal_data)
    yesterday_watchlist_df=get_yesterday_snapshot('watchlist_status')
    yesterday_action_signal_df=get_yesterday_snapshot('action_signal')
    today_saved_watchlist=save_snapshot(filtered_watchlist, 'watchlist_status')
    today_saved_action_signal=save_snapshot(sell_signal,'action_signal')
    (watchlist_unchanged,
     watchlist_changed,
     watchlist_exited,
     watchlist_new)=compute_ticker_transition(yesterday_watchlist_df,filtered_watchlist, 'watchlist_status')
    (action_signal_unchanged,
     action_signal_changed,
     action_signal_exited,
     action_signal_new)=compute_ticker_transition(yesterday_action_signal_df,sell_signal, 'action_signal')
    
    (watchlist_present_tickers,
     watchlist_absent_tickers)=missing_tickers_report(watchlist_exited,watchlist_data)
    (action_present_tickers,
     action_absent_tickers)=missing_tickers_report(action_signal_exited,sell_signal_data)

    watchlist_report=generate_watchlist_report(watchlist_changed,watchlist_new)
    action_report=generate_action_signal_report(action_signal_changed,action_signal_new)


    

    
    

    
