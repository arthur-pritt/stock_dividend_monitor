import logging
import pandas as pd
from datetime import date
from sqlalchemy.orm import Session
from database.models import Base 
from typing import Type 
from sqlalchemy import select
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

def get_available_dates()->set[date]:
    """
    inspect the dates available in StockDailyWatchlist
    Inspect the dates available in DividendYieldGain
    Determine which dates available in both datasets
    Return those common/available dates as date facts for resolve_target_date()
    """

    logger.info(f"Getting All the Available Dates For Streamlit")

    #Retrieve all the common dates in the table(watchlist and dividend)
    
    with get_session() as session:
        watchlist_query=select(
            StockDailyWatchlist.latest_date
        ).distinct()

        dividend_query=select(
            DividendYieldGain.latest_date
        ).distinct()

        watchlist_result=session.execute(watchlist_query)
        dividend_result=session.execute(dividend_query)

    watchlist_dates = set(watchlist_result.scalars())
    dividend_dates = set(dividend_result.scalars())

    common_dates= watchlist_dates & dividend_dates
    logger.info(f"Completed Getting Common Dates in the Two Datasets")

    return common_dates

def resolve_target_date(
        database_dates:set[date], 
        selected_date:date | None=None)->date:
    """
    Provide the lastest available ETL date as default
    Allow users to select the date.
    """
    logger.info("\nGetting the target dates===")
    #ETL_available dates as the baseline of dates actually available
    #if user has not selected a date, use ETL_available date as the default
    #if user has selected a date, check whether that date exists in common dates
    #if it exists- use that date as target_date
    #if it doesn't exists- No dates available
    #Return one concrete target-date
    
    if not database_dates:
        raise ValueError(f"The ETL Database has no dates")

    latest_etl_date=max(database_dates)

    if selected_date is None:
        target_date=latest_etl_date
        logger.info(f"Latest available ETL date is: {target_date}")
        return target_date

    if selected_date in database_dates:
        target_date=selected_date
        logger.info(f"Selected Date [{target_date}]Exists in the Database sets")
        return target_date
    else:
        logger.warning(f"The requested date isn't available")
        raise ValueError(f"{selected_date} doesn't exists in the database")



def load_data_once()->tuple[pd.DataFrame,pd.DataFrame]:
    """"
    Perform one complete attempt to obtain the data required by the Streamlit application.
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

    common_dates=get_available_dates()
    target_date=resolve_target_date(common_dates, selected_date=date(2026, 8, 25))

    #received_watchlist, received_dividend= load_streamlit_data()
    #print(received_watchlist)
    #print(received_dividend)
    
    


    