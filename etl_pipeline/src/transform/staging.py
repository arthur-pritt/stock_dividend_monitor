import pandas as pd
from functools import reduce 
from edgar import set_identity
import os 
from dotenv import load_dotenv
import pandera.pandas as pa


from config.logging_config import (
    get_logger,
    setup_logging
)

from etl_pipeline.src.extract._clean_nasdaq_list import get_nasdaq_list
from etl_pipeline.src.extract._fetch_stock_price import get_price_data
from etl_pipeline.src.extract._fetch_dividend_data import get_dividend_data
from etl_pipeline.src.extract._fetch_earnings_data import get_earning_data
from etl_pipeline.src.schema.ticker_schemas import CURRENT_PRICE_FILE_SCHEMA
from config.settings import DATA_COLS


logger =get_logger(__name__)
setup_logging()
load_dotenv()

def validate_data_list(df):
    """
    Validating the nasdaq_list of 300 tickers.
    """

    #Basic validation
    if df is None:
        raise ValueError(f"The input dataframe is None. Provide a valid dataframe")
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a pandas dataframe. Got {type(df).__name__}")
    if df.empty:
        raise ValueError(f"The dataframe is empty.")
    
    #Row count validation
    n_rows=len(df)
    if n_rows <110:
        raise ValueError(f"DataFrame has only {n_rows} rows. Minimum required is 110 rows. "
            f"Received {n_rows} rows.")
    elif 110 <= n_rows <=249:
        logger.warning(f"DataFrame has {n_rows} rows. This is below the ideal range of 250-300. "
            f"Pipeline will continue but results may be limited.")
    else:
        pass
    
    #Pandera validation
    try:
        validated_df=CURRENT_PRICE_FILE_SCHEMA.validate(df,lazy=True)
        logger.info(f"Pandera validation successfully passed")
    except pa.errors.SchemaErrors as err:
        logger.error(f"Pandera Validation Failed")
        logger.error(err.failure_cases)
        raise

    #Convert ticker to uppercase for use with yfinance
    validated_df[DATA_COLS['ticker']]=validated_df[DATA_COLS['ticker']].str.upper().str.strip()

    # Then drop NaNs in market_cap
    validated_df = validated_df.dropna(subset=["market_cap"])

    logger.info(f"Final validated dataset: {len(validated_df)} rows after dropping NaNs")
    
    return validated_df



def unified_ticker_table(
        prices_table,
        dividend_table,
        earning_table,
        data_list,
):
    """
    Merging four data sources(fetch_stock_price, dividend_data,nasdaq_list, earning_data)
    into a unified master table. The most recent quarter's dividend is attached to the daily price
    """

    #Simulating four dataframes

    prices_table = prices_table.drop(columns=['year','month'])
    dividend_table = dividend_table.drop(columns=['cik'])
    earning_table = earning_table.drop(columns=['cik','quarter','year'])

    dfs=[
        data_list,
        prices_table,
        dividend_table,
        earning_table
    ]

    #Prehandle the schemas and data types *before* reduction begins
    #Ensuring that the join key is strictly the same across all the dataset since it is a string to prevent bugs
    def sanitize_dataframe(df):
        if 'ticker' in df.columns:
            df['ticker'] = df['ticker'].astype(str)
        return df 
    
    # Apply sanitization to all dataframes
    clean_dfs = [sanitize_dataframe(df) for df in dfs]

    try:
        complete_stock_table = reduce(
            lambda left, right: pd.merge( #reduce and pd.merge used to have a maximum granular control over column-based joins.
                left,
                right,
                on='ticker',
                how='left'
            ),
           clean_dfs 
        )
        # Clean up any duplicated columns generated during the loop
        complete_stock_table= complete_stock_table.loc[:, ~complete_stock_table.columns.str.endswith('_dup')]
    except TypeError:
        logger.error("ERROR: The dataframe list was empty")
        complete_stock_table= pd.DataFrame() #Fallback state

    return complete_stock_table

def cleaning_stock_table(df):
    """"
    Investigates and cleans the data.
    """

    #Changing the values of column to title case
    df['name']=df['name'].str.title()

    report = []
    #Doing an audit to find missing values, duplicates, confirming datatype


    for col in df.columns:
        missing = df[col].isna().sum()
        duplicates = df[col].duplicated().sum()

        recommendations = []
        if missing:
            recommendations.append("Handle missing values")

        if duplicates:
            recommendations.append("Check for duplicates")

        if df[col].dtype == "object":
            recommendations.append("Check duplicates")

        if df[col].dtype == "Object":
            recommendations.append("Check text standardization")

        report.append({
            "column": col,
            "missing_count":missing,
            "duplicate_count":duplicates,
            "action_needed":
                   "Yes" if recommendations else "NO",
            "recommendations":
            ", ".join(recommendations)}
        )
        final_report = pd.DataFrame(report)

        return df

def audit_numeric_columns(df):

    report = []

    numeric_cols = df.select_dtypes(
        include="number"
    ).columns

    for col in numeric_cols:

        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)

        IQR = Q3 - Q1

        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        outliers = (
            (df[col] < lower) |
            (df[col] > upper)
        ).sum()

        report.append({
            "column": col,
            "outlier_count": outliers,
            "outliers_present":
                "YES" if outliers > 0 else "NO"
        })

    return pd.DataFrame(report)

if __name__ == "__main__":
    set_identity(os.environ.get("EDGAR_IDENTITY"))

    try:
        logger.info("====Starting to merge all four files===")

        final_list= get_nasdaq_list()
        staging_data = validate_data_list(final_list)
        prices_data=get_price_data(staging_data)
        dividend_data = get_dividend_data(staging_data)
        earning_data = get_earning_data(staging_data)
        ticker_table=unified_ticker_table(prices_data, dividend_data, earning_data, staging_data)
        audit_report = cleaning_stock_table(ticker_table)
        print(audit_report)
    

    except Exception as e:
        logger.error(f"Merging FAILED: {str(e)}")




