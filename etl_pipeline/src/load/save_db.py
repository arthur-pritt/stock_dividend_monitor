from sqlalchemy.orm import Session
import pandas as pd 
from database.models import (
    Stock,
    DailyStockPrice,
    DividendCompanies,
    NonDividendCompanies,
    Dividend, 
    Earnings,
    Historical90DaysData,
    StockDailySegmented,
 #   DividendYieldGain,
 #   StockDailyWatchlist,
    StockDailyFlat   
    
)
from service.stock_service import StockService
from service.daily_stock_service import DailyStockService
from service.dividend_companies_service import DividendCompaniesService
from service.nondividend_companies_service import NonDividendCompaniesService
from service.dividend_service import DividendService
from service.earning_service import EarningService
from service.historical_data_service import HistoricalDataService
from service.segmented_stocks_service import SegmentedStocksService
#from service.dividend_yieldcal_service import DividendYieldService
#from service.watchlist_stocks_service import WatchlistService
from service.stockdaily_service import StockDailyFService

from config.logging_config import get_logger
logger = get_logger(__name__)


def load_stocks(data:pd.DataFrame, session: Session)-> int:

    """
    Load stock DataFrame data into PostgreSQL.
    Args:
         data: Validated stock DataFrame.
         session: Active SQLAlchemy session.
    Returns the number of stocks loaded.
    
    """

    if data.empty:
        return 0

    records = data.to_dict('records')
    service =  StockService(session)
    return service.save_stocks(records)

def load_daily_stock_prices(data:pd.DataFrame, session:Session)-> int:
    """
    Load daily stock prices from a DataFrame
    into PostgreSQL.
    """

    if data.empty:
        return 0

    db_columns=['ticker','recorded_date','adj_close']
    clean_df=data[db_columns]
    
    records = clean_df.to_dict('records')
    service = DailyStockService(session)
    return service.save_prices(records)

def load_dividend_companies(data:pd.DataFrame, session:Session)-> int:
    """
    Load dividen companies into the PostgreSQL."""

    if data.empty:
        return 0

    records = data.to_dict('records')
    service = DividendCompaniesService(session)
    return service.save_stocks(records)

def load_non_dividend_companies(data:pd.DataFrame, session:Session)-> int:
    """
    Load non dividend companies into the PostgreSQL.
    """

    if data.empty:
        return 0

    db_columns =['ticker',
                 'recorded_date',
                 'name',
                 'market_cap',
                 'adj_close',
                 'earnings_pershare',
                 'dividend_status',
                 'quarter',
                 'year']
    clean_df=data[db_columns]

    records = clean_df.to_dict('records')
    service = NonDividendCompaniesService(session)
    return service.save_stocks(records)

def load_dividend_data(data:pd.DataFrame, session:Session)-> int:
    """
    Load dividend per share data for 600 nasdaq companies into postgreSQL.
    """

    if data.empty:
        return 0

    db_columns=['ticker', 'cik','dividend_per_share','raw_payout','quarter', 'year']
    clean_df=data[db_columns]

    records = clean_df.to_dict('records')
    service = DividendService(session)
    return service.save_stocks(records)

def load_earning_data(data:pd.DataFrame, session:Session)-> int:
    """
    Load earning per share data for 600 nasdaq companies into PostgreSQL
    """

    if data.empty:
        return 0

    records = data.to_dict('records')
    service = EarningService(session)
    return service.save_stocks(records)

def load_historical_data(data:pd.DataFrame, session:Session)-> int:
    """
    Load historical data into PostgreSQL.
    """
    if data.empty:
        return 0

    records = data.to_dict('records')
    service =HistoricalDataService(session)
    return service.save_stocks(records)
def load_segmented_tickers(data:pd.DataFrame, session:Session)-> int:
    """
    Loading segmented ticker sinto the PostgreSQL database.
    """

    if data.empty:
        return 0

    records = data.to_dict('records')
    service = SegmentedStocksService(session)
    return service.save_stocks(records)

##def load_dividend_yield_calculator(data:pd.DataFrame, session:Session)-> int:
    dividend_yield_cal=[
        DividendYieldGain(
            ticker = row['ticker'],
            current_date=row['current_date'],
            current_adjclose=row['current_adjclose'],
            historical_date=row['historical_date'],
            actual_days=row['actual_days'],
            price_diff=row['price_diff'],
            pct_change=row['pct_change'],
            watchlist_status=row['watchlist_status'],
            name=row['name'],
            market_cap=row['market_cap'],
            dividend_per_share=row['dividend_per_share'],
            raw_payout=row['raw_payout'],
            frequency=row['frequency'],
            quarter=row['quarter'],
            dividend_status=row['dividend_status'],
            dividend_yield_pct=row['dividend_yield_pct'],
            three_year_yield=row['three_year_yield'],
            five_year_yield=row['five_year_yield'],
            ten_year_yield=row['ten_year_yield'],
            three_year_pct=row['three_year_pct'],
            fie_year_pct=row['five_year_pct'],
            ten_year_pct=row['ten_year_pct'],
            action_signal=row['action_signal']

        )
        for _, row in data.iterrows()
    ]
    service = DividendYieldService(session)
    return service.save_stocks(dividend_yield_cal)

#def load_watchlist_service(data:pd.DataFrame, session:Session)-> int:
    watchlist=[
        StockDailyWatchlist(
            ticker=row['ticker'],
            name=row['name'],
            market_cap=row['market_cap'],
            latest_date=row['latest_date'],
            current_adjclose=row['current_adjclose'],
            dividend_per_share=row['dividend_per_share'],
            raw_payout=row['raw_payout'],
            earnings_per_share=row['earnings_per_share'],
            quarter=row['quarter'],
            year=row['year'],
            dividend_status=row['dividend_status'],
            baseline_date=row['baseline_date'],
            historical_adjclose=row['historical_adjclose'],
            actual_days=row['actual_days'],
            price_diff=row['price_diff'],
            pct_change=row['pct_change'],
            watchlist_status=row['watchlist_statu']

        )
        for _, row in data.iterrows()
    ]

    service= WatchlistService(session)
    return service.save_stocks(watchlist)

def load_complete_stock_table(data:pd.DataFrame, session:Session)-> int:
    """
    Loading complete dataframe into PostgreSQL """
    
    if data.empty:
        return 0

    data['raw_payout'] = data['raw_payout'].fillna(0)
    data['frequency'] = data['frequency'].fillna('None')
    data['quarter'] = data['quarter'].fillna(0).astype('Int64')
    data['year'] = data['year'].fillna(0).astype('Int64')

    records = data.to_dict('records')
    service = StockDailyFService(session)
    return service.save_stocks(records)


















