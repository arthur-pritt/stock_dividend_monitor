import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select
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

def get_todays_data(session:Session, model:Type[Base], target_date:date=None)->pd.DataFrame:
    """
    -Takes session and model as parameter.
    -Queries the supabase database to retrieve watchlist data
    -Convert database results into the dataframe
    -Validates the expected shape of the dataframe results
    -Returns the dataframe"""
    logger.info(f"STARTING: Getting today's data===")

    #Get today's date
    if target_date is None:
        target_date = date.today()

    #Retrieving all the record currently stored in the table(watchlist and capital gain table)
    #Specifying the table to work with using SQL statement object/building the question
    query_stmt=select(model.__table__).where(
        model.__table__.c.latest_date == target_date
    )

    #Establishing the connection to manage the unit of work/Executing the question
    result=session.execute(query_stmt)

    #Returning the answer to the question
    rows=result.mappings().all()
    rows=pd.DataFrame(rows)

    logger.info(f"COMPLETED:Today's Data already pulled from the database ")

    return rows

if __name__ == "__main__":
    logger.info(f"STARTED: Connecting & Reading Data From the Database====")
    with get_session() as session:
        watchlist_df=get_todays_data(session, StockDailyWatchlist)
        dividend_df=get_todays_data(session,DividendYieldGain)
    print(f"COMPLETED: Watchlist Data Already Pulled")
    print(watchlist_df)
    print(f"COMPLETED: Capital Gain For Dividend Earning Stocks")
    print(dividend_df)    


