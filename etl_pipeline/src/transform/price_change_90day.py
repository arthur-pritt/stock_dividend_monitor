import pandas as pd
import numpy as np


from config.logging_config import(
    setup_logging,
    get_logger
)

from etl_pipeline.src.extract._backfill import get_historical_data
from etl_pipeline.src.transform.stock_classification import get_classified_ticker_df
from config.settings import (
    PROCESSED_SUBDIR,
    WATCHLIST_STATUS_FILEPATH   
)

PROCESSED_SUBDIR.mkdir(parents=True, exist_ok=True)


setup_logging()
logger= get_logger(__name__)

def validating_two_dataframes(df,
                              name,
                              required_columns=None,
                              expected_schema=None,):
    
    """
    Validating incoming inputs from both classified_df and historical_df.
    """

    #Basic validation

    if df is None:
        raise ValueError(f"{name}: dataframe is None")
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f" {name}: expected Dataframe, got {type(df).__name__}")
    
    if df.empty:
        raise ValueError(f" {name}: Dataframe is empty")
    
    # Required columns check

    if required_columns:
        missing_columns = (
            set(required_columns)-set(df.columns)
        )

        if missing_columns:
            raise ValueError(
                f"{name}: missing columns"
                f"{sorted(missing_columns)}"
            )
        
    #Schema Conformance

    if expected_schema:
        for column, expected_dtype in expected_schema.items():
            if column not in df.columns:
                continue 

            actual_dtype = str(df[column].dtype)

            if actual_dtype != expected_dtype:
                raise TypeError(
                    f"{name}: column '{column}' "
                    f"expected {expected_dtype},"
                    f"got {actual_dtype}"
                )
            
    logger.info(f" PASSED: All validation for both historical data and classification data.")
    return True 


def price_change_calculation(
        classified_data:pd.DataFrame,
        historical_data:pd.DataFrame
)-> pd.DataFrame:
    """
    Calculate how much a stock has moved in the last 90 days and add a watchlist_status column.
    """

    #Light validation of the two dataframes(df isNone, df isinstance, dfempty).
    classified_data = classified_data.dropna(subset=['adj_close', 'date'])
    historical_data = historical_data.dropna(subset=['adjclose', 'date'])

    #For classified_data, grab the row with the maximum date max(date) per ticker or oldest

    classified_data= classified_data.sort_values(
        by=['ticker','date'],
        ascending= True
    ).drop_duplicates(subset='ticker', 
                      keep='last')
    classified_data=classified_data.reset_index(drop=True)

    #For historical data, grab the row with the minimum date min(date) per ticker
    historical_data=historical_data.sort_values(
        by=['ticker','date'],
        ascending=True
    ).drop_duplicates(subset='ticker',
                      keep='first')
    historical_data=historical_data.reset_index(drop=True)

    #Column renaming in both classified_data & historical_data
    #For classified_data, adj_close to current_adjclose, date to current_date
    #For historical_data, adjclose to historical_adjclose, date to historical_date
    classified_data= classified_data.rename(columns={"adj_close" :"current_adjclose"})
    classified_data= classified_data.rename(columns={"date" : "current_date"})

    historical_data= historical_data.rename(columns={'adjclose' : 'historical_adjclose'})
    historical_data= historical_data.rename(columns={'date' :'historical_date'})

    #Dropping of columns in the historical_date such as open, high, low, close, volume, actual_days,coverage_pct, and is flagged
    historical_data= historical_data.drop(columns=[
        'open',
        'high',
        'low',
        'close',
        'volume',
        'actual_days',
        'coverage_pct',
        'is_flagged'
    ],
    errors='ignore')

    #DATA QUALITY GATE:Zero values/10% error budget. If the percentage of zero values exists in the historical_data and classified_data and is more than 10
    #%, the pipeline stops. If less than that, the pipeline logs a warning and saves the bad data to zero_tickers.csv.
    #Bad rows dropped.
    historical_zero_pct= (historical_data['historical_adjclose']==0).mean()*100 
    classified_zero_pct= (classified_data['current_adjclose']==0).mean()*100

    historical_zero=(historical_data['historical_adjclose']==0)
    classified_zero=(classified_data['current_adjclose']==0)

    if historical_zero_pct >10.0 or classified_zero_pct >10.0:
        raise ValueError(f"Pipeline stopped: Zero price occurrence exceeds the 10% error budget.")
    
    if historical_zero_pct > 0 or classified_zero_pct > 0:
        logger.warning("Zero prices discovered under budget threshold. Logging anomalies to zero_tickers.csv")
        zero_tickers_csv=pd.concat(
            [historical_zero,classified_zero],
            ignore_index=True
        )
        if not zero_tickers_csv.empty:
            logger.warning(f"{len(zero_tickers_csv)} ticker(s) with zero adjusted close price.")
            zero_tickers_csv.to_csv(
                'zero_tickers.csv',
                index=False)

    historical_data=historical_data[historical_data['historical_adjclose'] !=0]
    classified_data=classified_data[classified_data['current_adjclose'] !=0]
    
    #Convert to the ticker column to uppercase
    classified_data['ticker']=classified_data['ticker'].str.upper()
    historical_data['ticker']=historical_data['ticker'].str.upper()
    

    #DATA QUALITY GATE:Identify which tickers exist in one slice but not the other. Export those discrepancies to failed_tickers.csv
    #Perform inner join between historical_data and classified_data.some entries here may be tickers already excluded upstream for zero-price — check zero_tickers.csv first
    result_df=classified_data.merge(
        historical_data,
        on='ticker',
        how='outer',
        indicator=True
    )
    failed_tickers = result_df[result_df['_merge'] !="both"]
    if not failed_tickers.empty:
        logger.warning(f"{len(failed_tickers)} ticker(s) failed the join.")
        failed_tickers.to_csv(
            'failed_tickers.csv',
            index=False
            )
        
    joined_df= result_df[result_df['_merge']=="both"].copy()
    logger.info(f"SUCCESS: joining historical data with classified data")


    #DATA QUALITY GATE: Calculate the delta between current_date and historical_date. Identify rows outside your acceptable calendar day threshold, log them to dropped_tickers.csv, and filter them out.
    #if the difference between min(date) and max(date) is less than 80 or 95 dats. And create actual_days column
    joined_df['current_date']=pd.to_datetime(joined_df['current_date'])
    joined_df['actual_days']= (joined_df['current_date'] - joined_df['historical_date']).dt.days

    mask = (joined_df['actual_days'] < 80) | (joined_df['actual_days'] > 95)
    dropped_tickers = joined_df[mask]

    if not dropped_tickers.empty:
        logger.info(f"Dropping tickers that fall outside the 80 to 95 calendar window")
        dropped_tickers.to_csv(
            'dropped_tickers.csv',
            index=False
            )

    joined_df=joined_df[~mask]

    #Math & Logic Engine: Safely compute pct_change (since zeros are gone) and apply your conditional labeling for the watchlist_status

    joined_df['price_diff']= joined_df['current_adjclose'] - joined_df['historical_adjclose']
    joined_df['pct_change']= joined_df['price_diff'] / joined_df['historical_adjclose'] *100

    conditions = [
        joined_df['pct_change'] >=50.00,

        (joined_df['pct_change'] >-20.0) &
        (joined_df['pct_change'] <50.0),

        joined_df['pct_change'] <=-20.0
    ]

    choices = [
        "SKYROCKET",
        "NORMAL",
        "DROP"
    ]

    joined_df['watchlist_status']= np.select(
        conditions,
        choices,
        default='Unknown'
    )

    logger.info("\n======Watchlist_status row counts=====")
    logger.info(
        joined_df['watchlist_status'].value_counts())
    
    return joined_df
    
def get_watchlist_status()-> pd.DataFrame:
    
    """
    Orchestrates the entire files and saves the watchlist status results
    to watchlist_status.csv."""

    # Calculating the 90 day price change and saving the watchlist_status

    historical_data = get_historical_data()
    classified_data = get_classified_ticker_df()

    dataframes = {
        'historical_df': historical_data,
        'classified_df': classified_data
                }
    for name, df in dataframes.items():
        validating_two_dataframes(df,name)
    
    watch_status_df = price_change_calculation(classified_data,historical_data)
    watch_status_df.to_csv(
        WATCHLIST_STATUS_FILEPATH,
        index=False,
        float_format= "%.2f",
        na_rep="NA",
        encoding="utf-8"
    )
    
    logger.info("Pipeline Executed successfuly. WATCHLIST_STATUS table saved to csv file")
    return watch_status_df 


if __name__ == "__main__":
    try:
        watchlist_table=get_watchlist_status()
        print(watchlist_table)

    except Exception as e:
        logger.error(f"Math logic failed: {str(e)}")
    



   