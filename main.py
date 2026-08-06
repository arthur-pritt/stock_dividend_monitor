import logging 
import pandas as pd
from database.session import get_session
from datetime import datetime
from config.logging_config import(
    setup_logging,
    get_logger
)

#Initializing the logging system
setup_logging()

#Initializing get logger
logger=get_logger(__name__)

#Importing config files
from config.settings import(
    NASDAQ_LIST_FILEPATH,
    DAILY_PRICE_FILEPATH,
    DIVIDENDS_FILEPATH,
    EARNINGS_FILEPATH,
    BACKFILL_FILEPATH,
    STAGING_FILEPATH,
    CACHED_DIVIDEND_FILEPATH, 
    CACHED_NON_DIVIDEND_FILEPATH,
    CLASSIFICATION_FILEPATH,
    WATCHLIST_STATUS_FILEPATH,
    CAPITAL_GAIN_FILEPATH
    
)

from etl_pipeline.src.load.save_db import (
    load_stocks,
    load_daily_stock_prices,
    load_dividend_data,
    load_earning_data,
    load_historical_data,
    load_complete_stock_table,
    load_dividend_companies,
    load_non_dividend_companies,
    load_segmented_tickers,
    load_watchlist_service,
    load_dividend_yield_calculator
    
)


def run_pipeline():
    logging.info("=" * 60)
    logging.info("Starting Capital Gain Monitor Pipeline...")
    logger.info("=" * 60)

    #1:Extracting files

    #Clean 600 Nasdaq Stock Tickers
    logger.info(f"Loading pre-cleaned stock list from {NASDAQ_LIST_FILEPATH}")
    clean_ticker_list=pd.read_csv(NASDAQ_LIST_FILEPATH)

    #Daily Ticker Prices
    logger.info(f"Loading daily stock prices from {DAILY_PRICE_FILEPATH}")
    daily_ticker_prices= pd.read_csv(DAILY_PRICE_FILEPATH)

    #Quarterly Dividend Per Share Data
    logger.info(f"Loading dividend per share data from {DIVIDENDS_FILEPATH} ")
    dividend_per_share=pd.read_csv(DIVIDENDS_FILEPATH)

    #Earning Per Share Data
    logger.info(f"Loading earnings per share data from {EARNINGS_FILEPATH}")
    earning_pershare=pd.read_csv(EARNINGS_FILEPATH)

    #Historical Data
    logger.info(f"Loading historical data from {BACKFILL_FILEPATH}")
    historical_data=pd.read_csv(BACKFILL_FILEPATH)

    #2:Staging Stage

    #Complete joined ticker table
    logger.info(f"Loading raw and joined ticker table from {STAGING_FILEPATH}")
    raw_joined_table=pd.read_csv(STAGING_FILEPATH)

    #3:Transforming files
    #Dividend paying companies
    logger.info(f"Loading dividend paying companies from {CACHED_DIVIDEND_FILEPATH}")
    dividend_companies=pd.read_csv(CACHED_DIVIDEND_FILEPATH)

    #NonDividend Paying Companies
    logger.info(f"Loading non dividend companies from {CACHED_NON_DIVIDEND_FILEPATH}")
    non_dividend_companies=pd.read_csv(CACHED_NON_DIVIDEND_FILEPATH)

    #Segmented ticker table
    logger.info(f" Loading classified ticker table from {CLASSIFICATION_FILEPATH}")
    segmented_tickers= pd.read_csv(CLASSIFICATION_FILEPATH)

    #90 Day Price Change & Watchlist Table
    logger.info(f"Loading the 90 day price change & watchlist from {WATCHLIST_STATUS_FILEPATH}")
    ninety_day_table= pd.read_csv(WATCHLIST_STATUS_FILEPATH)

    #Capital Gain Calculator Table
    logger.info(f"Loading capital gain calculator from {CAPITAL_GAIN_FILEPATH}")
    capital_gain_calculator = pd.read_csv(CAPITAL_GAIN_FILEPATH)

    #4:Loading files into Supabase in single atomic transaction session
    
    
    with get_session()as session:
        #----VERIFY TABLE 1: Ticker List----
        logging.info("Loading master ticker table list...")
        stocks_count=load_stocks(clean_ticker_list, session)
        logger.info(f"Loaded {stocks_count} tickers into the Supabase database")

        #----VERIFT TABLE 2 Daily Ticker Prices----
        logging.info("Loading Daily Stock Prices....")
        prices_count= load_daily_stock_prices(daily_ticker_prices, session)
        logger.info(f"Loaded {prices_count} tickers prices into the Supabase database")

        #----VERIFY TABLE 3 Dividend Per Share Tables----
        logging.info("Loading dividend per share table....")
        dividend_count= load_dividend_data(dividend_per_share, session)
        logger.info(f"Loaded {dividend_count} tickers with dividend per share value into database")

        #----VERIFY TABLE 4 Earning per Share Table---
        logging.info("Loading earning per share table....")
        earning_count = load_earning_data(earning_pershare, session)
        logger.info(f"Loaded {earning_count} earning per share prices into the database")

        #---VERIFY TABLE 5 Historical Table----
        logging.info("Loading historical data table---")
        historical_count = load_historical_data(historical_data, session)
        logger.info(f"Loaded {historical_count} tickers with 90 days data")
        logger.info(f"Loaded {historical_count} tickers with 90 days data into the database")

        #---VERIFY TABLE 6 Complete Ticker Table----
        logging.info("Loading complete raw & joined ticker table----")
        complete_table_count= load_complete_stock_table(raw_joined_table, session)
        logger.info(f"Loaded {complete_table_count} raw joined data with all ticker info")
        logger.info(f"Loaded {complete_table_count} into the database")

        #---VERIFY TABLE 7 Dividend Companies Table----
        logger.info(f"Loading dividend paying companies----")
        dividend_companies_count = load_dividend_companies(dividend_companies, session)
        logger.info(f"Loaded {dividend_companies_count} dividend paying companies")
        logger.info(f"Loaded {dividend_companies_count} dividend paying companies into the database")


        #---VERIFY TABLE 8 Non dividend companies table---
        logging.info("Loading non dividend paying companies-----")
        non_dividend_companies = load_non_dividend_companies(non_dividend_companies, session)
        logger.info(f"Loaded {non_dividend_companies} non dividend paying companies")
        logger.info(f"Loaded {non_dividend_companies} non dividend paying companies into the database.")

        #---VERIFY TABLE 9 Classified Dividend Ticker Table----
        logging.info(f"Loading segmented ticker table-----")
        segmented_tickers_count= load_segmented_tickers(segmented_tickers, session)
        logger.info(f"Loaded {segmented_tickers_count} segmented ticker table")
        logger.info(f"Loaded {segmented_tickers_count} into the database")


        #----VERIFY TABLE 10 Ninety 90 Price Change----
        logging.info(f"Loading the 90 day price change table---")
        ticker_count=load_watchlist_service(ninety_day_table, session)
        logging.info(f"Loaded {ticker_count} tickers in the 90 day price change table")
        logging.info(f"Loaded {ticker_count} tickers into the Supabase database")

        #----VERIFT TABLE 11 Capital Gain Calculator----
        logger.info(f"Loading Capital Gain Threshold & Yield")
        capital_gain_count=load_dividend_yield_calculator(capital_gain_calculator, session)
        logger.info(f"Loaded {capital_gain_count} tickers with capital gain threhsold")
        logger.info(f"Loaded {capital_gain_count} tickers into the database")

    logging.info("Pipeline run completed successfully!")

if __name__ == "__main__":
    run_pipeline() 


