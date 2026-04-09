import pandas as pd
import pandera.pandas as pa
from yahooquery import Ticker

#importing config files
from config.logging_config import get_logger
from etl_pipeline.src.extract._clean_nasdaq_data import pre_validate_with_yahoo
from config.settings import (
    DATA_COLS
)

#Getting the logger for the module
logger=get_logger(__name__)

def validate_tickers(df, min_rows=200):
    """
    Validating the pandas input that has 3 columns and 300 rows."""

    #Confirm pandas
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a pandas dataframe but got {type(df).__name__}")
    
    #confirm none or empty
    if df is None or df.empty:
        raise ValueError(f" The dataframe is empty or none")
    
    #confirm rows
    if df.shape[0]<min_rows:
        raise ValueError(f"Not enough data. Expected at least 200, got {len(df)}")
    
    #confirm column
    if df.shape[1]<3:
        raise ValueError(f"Insufficient columns. Expected at least 3 column, got {len(df)}")
    
    #required column check
    required_col={DATA_COLS['ticker'], DATA_COLS['name'], DATA_COLS['valuations']}
    missing_col= []

    for col in required_col:
        if col not in df.columns:
            missing_col.append(col)
    
    if missing_col:
        raise ValueError(f" Required columns are missing. Confirm before proceedinng")
    
    print("LOG: Data validation successful. Input meets all structural requirements.")
    return df 

if __name__ == "__main__":
    from config.logging_config import setup_logging
    from etl_pipeline.src.extract._clean_nasdaq_data import (
        validateInData, extract_columns, normalize_names,
        build_master_list, match_and_categorize,
        get_top_300, validate_top_300, pre_validate_with_yahoo
    )
    from etl_pipeline.src.extract._download_nasdaq_list import load_nasdaq_data

    setup_logging()
    logger.info("Testing validate_tickers...")

    # Run the chain to get data
    df = load_nasdaq_data()
    validated_data = validateInData(df)
    final_three_columns = extract_columns(validated_data)
    normalized_df = normalize_names(final_three_columns)
    master_reference = build_master_list(normalized_df)
    final_categorized_df = match_and_categorize(normalized_df, master_reference)
    top_300 = get_top_300(final_categorized_df)
    validated_top_300 = validate_top_300(top_300)

    # Now test YOUR function
    result = validate_tickers(validated_top_300)
    logger.info(f"Result shape: {result.shape}")
    logger.info(f"Columns: {result.columns.tolist()}")
    logger.info(result.head())



    


    




