import logging
import pandas as pd
from database.session import get_session
from database.models import(
    StockDailyWatchlist,
    DividendYieldGain
)

from config.logging_config import (
    setup_logging,
    get_logger
)

from utilis.retry_operation import retry_operation

#Initializing the logging system
setup_logging()

#Initializing get logger
logger=get_logger(__name__)

from data_access import get_data_for_date


def load_data_once()->tuple[pd.DataFrame,pd.DataFrame]:
    """"
    Centralize the operations of obtaining data for streamlit use and returning them.
    """

    logger.info(f"\nSTARTING:Receiving data to be consuming by streamlit")

    with get_session() as session:
        watchlist_df=get_data_for_date(session, StockDailyWatchlist)
        dividend_df=get_data_for_date(session,DividendYieldGain)

    logger.info("\nCOMPLETE: ALL data received and Ready")

    return watchlist_df, dividend_df

def load_streamlit_data():
    """
    Load streamlit data using the configured retry meechanisms"""
    return retry_operation(load_data_once)

    
if __name__ == "__main__":
    logger.info(f"\nReceiving data and Component to be consumed by Streamlit")

    received_watchlist, received_dividend= load_streamlit_data()
    print(received_watchlist)
    print(received_dividend)
    
    


    