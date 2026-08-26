import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import (
    select,
    func
)
from database.models import Base
from typing import Type
from database.session import get_session 
from database.models import(
    StockDailyWatchlist,
    DividendYieldGain
)
from datetime import date
from config.logging_config import (
    setup_logging,
    get_logger
)

#Initializing the logging system
setup_logging()

#Initializing get logger
logger=get_logger(__name__)

def get_data_for_date(session:Session, model:Type[Base], target_date:date=None)->pd.DataFrame:
    """
    -Takes session and model as parameter.
    -Queries the supabase database to retrieve watchlist data
    -Convert database results into the dataframe
    -Validates the expected shape of the dataframe results
    -Returns the dataframe"""
    logger.info(f"STARTING: Getting today's data===")

    #Get today's date
    if target_date is None:
        target_date=date.today()

    logger.info(f"Expected Date:{target_date}")

    #Retrieving all the record currently stored in the table(watchlist and capital gain table)
    #Specifying the table to work with using SQL statement object/building the question
    query_stmt=select(model.__table__).where(
        model.__table__.c.latest_date == target_date
    )

    #Establishing the connection to manage the unit of work/Executing the question
    result=session.execute(query_stmt)

    #Returning the answer to the question
    rows=result.mappings().all()
    
    expected_columns=model.__table__.columns.keys()
    if rows:
        rows=pd.DataFrame(rows)
    else:
        rows=pd.DataFrame(columns=expected_columns)

    logger.info(f"\nValidating the final dataframe===")
    if len(expected_columns) != len(rows.columns):
        logger.warning(f"Number of columns don't match. Model has: {len(expected_columns)} Final dataframe has: {len(rows.columns)}")

    missing_columns=set(expected_columns)-set(rows.columns)
    extra_columns=set(rows.columns) - set(expected_columns)


    if missing_columns or extra_columns:
        raise ValueError(
            f"Invalid data structure:"
            f"Missing: {missing_columns}."
            f"Extra: {extra_columns}"
        )

    elif rows.empty:
        logger.warning(f"Data structure is valid but no records return")
        logger.warning(f"Columns :{list(rows.columns)}")
        logger.warning(f"rows : {rows.shape[0]}")

    else:
        logger.info(f"The data matches expected columns and records")
        logger.info(f"rows : {rows.shape[0]}")
        logger.info(f"Columns: {list(rows.columns)}")


    return rows

if __name__ == "__main__":
    logger.info(f"STARTED: Connecting & Reading Data From the Database====")
    with get_session() as session:
        watchlist_df=get_data_for_date(session, StockDailyWatchlist,target_date=date(2026,8,24))
        dividend_df=get_data_for_date(session, DividendYieldGain,target_date=date(2026,8,20))
    print(f"COMPLETED: Watchlist Data Already Pulled")
    #print(watchlist_df)
    print(f"COMPLETED: Capital Gain For Dividend Earning Stocks")
    #print(dividend_df)    


