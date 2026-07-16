import pandas as pd 
import pathlib
from pathlib import Path

from config.logging_config import (
    setup_logging,
    get_logger
)

from config.settings import (
    PROCESSED_SUBDIR,
    DIVIDENDS_FILEPATH,
    CACHED_DIVIDEND_FILEPATH
)

PROCESSED_SUBDIR.mkdir(parents=True, exist_ok=True)
setup_logging()
logger= get_logger(__name__)


def validating_dividend_dats():
    pass 

def calculating_dividend_yield(
        raw_dividend_df:pathlib.Path,
        dividend_paying_df:pathlib.Path)-> pd.DataFrame:
    """
    Calculates if a stock dividend yield equals three years,
    five years, and ten years and returns a complete 
    dataframe."""

    #Light validation

    #For raw_dividend_df, grab the row with the maximum date max(date) per ticker or newest date
    #Loading and reading data from the disk
    dividend_paying_df=pd.read_csv(
        CACHED_DIVIDEND_FILEPATH
    )
    dividend_paying_df=dividend_paying_df.sort_values(
        by=['ticker','date'],
        ascending=True
        ).drop_duplicates(subset='ticker',
                          keep='last')
    dividend_paying_df=dividend_paying_df.reset_index(drop=True)
    

    #Drop columns in raw_dividend_df such as CIK. Drop columns in  dividend_paying_df 
    #such as dividend_status, frequency. Frequency will be provided by raw_dividend_df
    raw_dividend_df= pd.read_csv(
        DIVIDENDS_FILEPATH
    )
    raw_dividend_df= raw_dividend_df.drop(columns=
                                          ['cik'])
    
    dividend_paying_df= dividend_paying_df.drop(columns=
                                                ['dividend_status',
                                                 'frequency',
                                                 'quarter',
                                                 'year',
                                                 'raw_payout',
                                                 'dividend_per_share'])

   
    #DATA QUALITY GATE: Zero values/10% Error budget. If the percentage of zero
    #exists in raw_dividend_df dividend_per_share column and is more than 10%,
    #the pipeline stops. Less than that, the pipeline proceeds and saves the report
    #to zero_dividend.csv. The same treatment for dividend_paying_df on the
    #adjusted close.

    dividend_paying_zero=(dividend_paying_df['adj_close']==0)
    dividend_paying_zero_pct=(dividend_paying_df['adj_close']==0).mean()*100

    if dividend_paying_zero_pct > 10.0:
        raise ValueError(f"Pipeline Stopped: Zero price occurrence excceds the 10% error budget.")
    
    if dividend_paying_zero_pct > 0:
        logger.warning("Zero prices discovered under budget threshold. LOgging anomalies to zero_dividend.csv")
        zero_dividend_csv=dividend_paying_df[dividend_paying_zero]
        if not zero_dividend_csv.empty:
            zero_dividend_csv.to_csv(
                dividend_paying_df.parent / "zero_dividend_data.csv",
                index=False,
                encoding='utf-8')
            
    #Keeping only clean data in memory
    dividend_paying_df = dividend_paying_df[~dividend_paying_zero]

    #DATA QUALITY GATE: Tickers that fail the join will be saved to
    #failed_dividend_csv. Perform inner join between raw_dividend_df and
    #dividend_paying_df.
    joined_dividend_df= dividend_paying_df.merge(
        raw_dividend_df,
        on='ticker',
        how='outer',
        indicator=True
    )
    failed_dividends= joined_dividend_df[joined_dividend_df['_merge'] !='both']
    if not failed_dividends.empty:
        logger.warning(f"{len(failed_dividends)} ticker(s) failed the join.")
        failed_dividends.to_csv(
            'failed_dividend.csv',
            index=False
        )
    joined_dividend_df=joined_dividend_df[joined_dividend_df['_merge']=="both"].copy()
    logger.info(f"SUCCESS: joining two dataset complete.")

    #ISOLATE AND CLEAN THE DATA:REMOVING NAN

    clean_mask=(joined_dividend_df['_merge']=="both") & joined_dividend_df['adj_close'].notna()
    clean_dividend_df=joined_dividend_df[clean_mask].copy()
    nan_dividend_df=joined_dividend_df[~clean_mask]
    if not nan_dividend_df.empty:
        print(f"Isolated {len(nan_dividend_df)} rows with missing market data")


    #Maths and Engine logic compute: Dividend_per_share is annualized
    #three_year_yield= dividend_per_share * 3
    #five_year_yield = dividend_per_share * 5
    #ten_year_yield = dividend_per_share * 10
    #three_year_yield_pct= (three_year_yield/adjusted_close) * 100
    #five_year_yield_pct=(five_year_yield/adjusted_close) * 100
    #ten_year_yield_pct=(ten_year_yield/adjusted_close) * 100
    #All these will be three columns with exact figure.

    clean_dividend_df['dividend_yield']=(clean_dividend_df['dividend_per_share'] /
                                         clean_dividend_df['adj_close']) * 100
    clean_dividend_df['three_year_yield']=clean_dividend_df['dividend_per_share']*3
    clean_dividend_df['five_year_yield']=clean_dividend_df['dividend_per_share'] * 5
    clean_dividend_df['ten_year_yield']=clean_dividend_df['dividend_per_share'] * 10

    clean_dividend_df['three_year_yield_pct']=(clean_dividend_df['three_year_yield'] /
                                               clean_dividend_df['adj_close']) * 100
    clean_dividend_df['five_year_yield_pct']=(clean_dividend_df['five_year_yield'] /
                                              clean_dividend_df['adj_close']) * 100

    clean_dividend_df['ten_year_yield_pct']=(clean_dividend_df['ten_year_yield'] /
                                             clean_dividend_df['adj_close']) * 100

    print(clean_dividend_df)


    return raw_dividend_df

if __name__== "__main__":
      try:

        logger.info(f"======CALCULATING DIVIDEND YIELD")
        dividend_info_path = pd.read_csv(DIVIDENDS_FILEPATH)
        dividend_companies_path = pd.read_csv(CACHED_DIVIDEND_FILEPATH)
        calculator = calculating_dividend_yield(dividend_info_path,dividend_companies_path)

        
      except Exception as e:
        logger.error(f"DIVIDEND CALCULATION FAILED: {str(e)}")

