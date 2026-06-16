import pandas as pd
import numpy as np


from config.logging_config import(
    get_logger,
    setup_logging
)

from etl_pipeline.src.transform.staging import get_stock_table

logger = get_logger(__name__)
setup_logging()

def validating_stock_data(staging_df):
    """
    Validating of the complete stock table data.
    """

    #Check/confirm if the data is None
    if staging_df is None:
        raise ValueError(" No data provided")
    
    #Check/confirm the data type is pandas
    if not isinstance(staging_df, pd.DataFrame):
        raise TypeError(f"Expected a pandas dataframe but got {type(staging_df).__name__}")
    
    #Check/confirm if the data type is empty
    if staging_df.empty:
        raise ValueError("Got an empty pandas dataframe")
    
    #Confirm/check the minimum number of rows
    n_rows =len(staging_df)

    if n_rows < 110:
        raise ValueError(f" Dataframe has only {n_rows} rows. Minimum required is 110 rows."
                         f"Received {n_rows} rows.")
    elif 110 <= n_rows <=300:
        logger.warning(f" DataFrame has {n_rows} rows. This is below the ideal range of 300 to 600"
                       f" Pipeline continues but results are limited.")
    else:
        pass 

    #confirm the required columns
    required_cols=['name', 'ticker', 'market_cap', 'dividend_per_share','adj_close','earnings_pershare', 'date']
    missing_col=[]

    for col in required_cols:
        if col not in staging_df:
            missing_col.append(col)
    
    if missing_col:
        raise ValueError(f" The data is missing key columns{required_cols}")
    
    return staging_df

def classify_stock_table(validated_stock_table):
    """
    A function that segments the companies that pay dividend and those that don't pay_dividend and returns a new column called dividend_status with
    values such as 'pays_dividend' and 'no_dividend'
    """

    #Validation

    if validated_stock_table is None:
        raise ValueError(f" No data provided.")
    
    if not isinstance(validated_stock_table, pd.DataFrame):
        raise TypeError(f" Expected a pandas dataframe, but got {type(validated_stock_table).__name__}")
    
    if validated_stock_table.empty:
        raise ValueError(f" The dataframe is empty.")

    #Segmenting tickers that pay dividend and non paying dividends
    validated_stock_table['dividend_status']= np.where(
        validated_stock_table['dividend_per_share'] < 0,
        "invalid_dividend",
        np.where(
            validated_stock_table['dividend_per_share']== 0,
            "no_dividend_payer",
            "dividend_payer"
        ) 
    )

    return validated_stock_table

if __name__ == "__main__":
    try:
        logger.info("====Starting to classify the tickers===")
        ticker_table=get_stock_table()
        ticker_identity= validating_stock_data(ticker_table)
        segmented_tickers= classify_stock_table(ticker_identity)
        print("\n====PIPELINE SUCCESS===")
        print(segmented_tickers[50:100])
        

    except Exception as e:
        logger.error(f"Classfication FAILED: {str(e)}")
    

