from sqlalchemy.orm import Session
from database.models import (
    Stock,
    DailyStockPrice
    
)
from service.stock_service import StockService
from service.daily_stock_service import DailyStockService
import pandas as pd 

def load_stocks(data:pd.DataFrame, session: Session)-> int:

    """
    Load stock DataFrame data into PostgreSQL.
    Args:
         data: Validated stock DataFrame.
         session: Active SQLAlchemy session.
    Returns the number of stocks loaded.
    
    """

    stocks = [
        Stock(
            ticker=row['ticker'],
            name=row['name'],
            market_cap=row['market_cap']
        )
        for _, row in data.iterrows()
    ]

    service =  StockService(session)
    return service.save_stocks(stocks)

def load_daily_stock_prices(data:pd.DataFrame, session:Session)-> int:
    """
    Load daily stock prices from a DataFrame
    into PostgreSQL.
    """

    prices=[
        DailyStockPrice(
            ticker=row['ticker'],
            recorded_date= row['recorded_date'],
            adj_close= row['adj_close'],
            month= row['month'],
            year = row['year']
        )
        for _, row in data.iterrows()
    ]
    service = DailyStockService(session)
    return service.save_prices(dailystockprices)






