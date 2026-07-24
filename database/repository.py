import sqlalchemy as sa
from database.connection import create_engine

metadata = sa.MetaData()
metadata.reflect(bind=
                 create_engine,
                 only=[
                     'stocks',
                     'daily_stock_prices',
                     'historical_90days_data',
                     'dividend',
                     'earnings',
                     'stock_daily_flat',
                     'stock_daily_segmented',
                     'stock_daily_watchlist',
                     'dividend_yield_gain',
                     'dividend_companies',
                     'non_dividend_companies'

                 ])

TABLES = metadata.tables
