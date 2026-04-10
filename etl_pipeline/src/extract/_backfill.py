import pandas as pd
import pandera.pandas as pa
from yahooquery import Ticker


from etl_pipeline.src.schema.ticker_schemas import TICKER_SCHEMA
#importing config files
from config.logging_config import get_logger
from config.settings import (
    DATA_COLS
)

#Getting the logger for the module
logger=get_logger(__name__)

def validate_tickers(df):
    return TICKER_SCHEMA.validate(df)

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



    


    




