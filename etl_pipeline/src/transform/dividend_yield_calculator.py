import pandas as pd 
import pathlib
from pathlib import Path
import numpy as np

from config.logging_config import (
    setup_logging,
    get_logger
)

from config.settings import (
    PROCESSED_SUBDIR,
    WATCHLIST_STATUS_FILEPATH,
    CACHED_DIVIDEND_FILEPATH
)

PROCESSED_SUBDIR.mkdir(parents=True, exist_ok=True)
setup_logging()
logger= get_logger(__name__)


def validating_dividend_dats():
    pass 

def calculating_dividend_yield(
        watchlist_path:pathlib.Path,
        dividend_paying_path:pathlib.Path)-> pd.DataFrame:
    """
    Calculates if a stock dividend yield equals three years,
    five years, and ten years and returns a complete 
    dataframe."""

    #Light validation

    #loading the pre_calculated 90-day price data and dividends
    watchlist_path = pd.read_csv(WATCHLIST_STATUS_FILEPATH)
    dividend_paying_path=pd.read_csv(CACHED_DIVIDEND_FILEPATH)

    #For dividend_payingdf, grab the row with the maximum date max(date) per ticker or newest date
    #Loading and reading data from the disk

    dividend_paying_path=dividend_paying_path.sort_values(
        by=['ticker','date'],
        ascending=True
        ).drop_duplicates(subset='ticker',
                          keep='last')
    dividend_paying_path=dividend_paying_path.reset_index(drop=True)

    #Drop columns in dividend_paying df such as dividend_status,date,adj_close,dividend_per_share,
    # . Drop columns in  dividend_paying_df 
    #such as dividend_status, frequency. Frequency will be provided by raw_dividend_df
    
    dividend_paying_path= dividend_paying_path.drop(columns=
                                                    ['date',
                                                     'adj_close',
                                                     'year',
                                                     'earnings_pershare'])
    
    watchlist_path= watchlist_path.drop(columns=
                                                ['dividend_status',
                                                 'frequency',
                                                 'quarter',
                                                 'year',
                                                 'raw_payout',
                                                 'dividend_per_share',
                                                 '_merge',
                                                 'name',
                                                 'market_cap',
                                                 'earnings_pershare'])
    
   
    #merging them on ticker
    merged_dividend_data=watchlist_path.merge(
        dividend_paying_path,
        on='ticker',
        how='inner'
    )

    #Calculation

    #Maths and Engine logic compute: Dividend_per_share is annualized
    #three_year_yield= dividend_per_share * 3
    #five_year_yield = dividend_per_share * 5
    #ten_year_yield = dividend_per_share * 10
    #three_year_yield_pct= (three_year_yield/adjusted_close) * 100
    #five_year_yield_pct=(five_year_yield/adjusted_close) * 100
    #ten_year_yield_pct=(ten_year_yield/adjusted_close) * 100
    #All these will be three columns with exact figure.

    merged_dividend_data['dividend_yield_pct']=(merged_dividend_data['dividend_per_share'] /
                                         merged_dividend_data['price_diff']) * 100
    merged_dividend_data['three_year_yield']=merged_dividend_data['dividend_per_share']*3
    merged_dividend_data['five_year_yield']=merged_dividend_data['dividend_per_share'] * 5
    merged_dividend_data['ten_year_yield']=merged_dividend_data['dividend_per_share'] * 10

    merged_dividend_data['three_year_yield_pct']=(merged_dividend_data['three_year_yield'] /
                                               merged_dividend_data['current_adjclose']) * 100
    merged_dividend_data['five_year_yield_pct']=(merged_dividend_data['five_year_yield'] /
                                              merged_dividend_data['current_adjclose']) * 100

    merged_dividend_data['ten_year_yield_pct']=(merged_dividend_data['ten_year_yield'] /
                                             merged_dividend_data['current_adjclose']) * 100
    
    print(merged_dividend_data)

    return merged_dividend_data

def action_signal(calculator:pd.DataFrame)-> pd.DataFrame:
    """
    Evaluates actual price gain meeting different dividend yields
    (3,5,10) and returning different label(
    SELL_PARTIAL_STAGE_3_20_PCT,
    SELL_PARTIAL_STAGE_3_30_PCT,
    SELL_PARTIAL_STAGE_1_30_PCT,
    HOLD)"""

    #Create temporary boolean series (in memory only, not saved as df columns)
    is_10yr= calculator['price_diff']>= calculator['ten_year_yield']
    is_5yr=  calculator['price_diff']>= calculator['five_year_yield']
    is_3yr=  calculator['price_diff']>= calculator['three_year_yield']

    #.Defining conditions from highest to lowest
    conditions = [
        is_10yr,
        is_5yr,
        is_3yr
    ]

    #.Matching labels
    choices =[
        "SELL_PARTIAL_STAGE_3_20_PCT",
        "SELL_PARTIAL_STAGE_2_30_PCT",
        "SELL_PARTIAL_STAGE_1_30_PCT"
    ]

    calculator['action_signal']=np.select(conditions,
                                          choices,
                                          default="HOLD")
    print(calculator[['ticker','pct_change','watchlist_status', 'action_signal']][0:50])
    print(calculator.columns)
    
    return calculator

if __name__== "__main__":
      try:

        logger.info(f"======CALCULATING DIVIDEND YIELD")
        price_change_info = pd.read_csv(WATCHLIST_STATUS_FILEPATH)
        dividend_companies_path = pd.read_csv(CACHED_DIVIDEND_FILEPATH)
        calculating_yield = calculating_dividend_yield(price_change_info,dividend_companies_path)
        signal_holder= action_signal(calculating_yield)

        
      except Exception as e:
        logger.error(f"DIVIDEND CALCULATION FAILED: {str(e)}")

