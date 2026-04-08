import pandas as pd
import re
from rapidfuzz import process,fuzz
from yahooquery import Ticker 
import pandera.pandas as pa


#Importing config files
from config.logging_config import get_logger 
from etl_pipeline.src.extract._download_nasdaq_list import load_nasdaq_data
from config.settings import (DATA_COLS,
                             INTERNAL_COLS,
                             CLEANING_PATTERNS,
                             CLEANING_REPLACEMENTS,
                             THRESHOLDS,
                             SYMBOL_EXCLUSION_REGEX,
                             MARKET_CAP_AGG,
                             LABELS
                             )

#Getting the logger for this module
logger = get_logger(__name__)
df=load_nasdaq_data()

def get_nasdaq_schema(min_rows=200):
        return pa.DataFrameSchema(
            columns={
                DATA_COLS['ticker']: pa.Column(str, nullable=False),
                DATA_COLS['name']:pa.Column(str, nullable=False),
                DATA_COLS['valuations']:pa.Column(float, nullable=True)

            },
            checks=pa.Check(lambda df:len(df) >=min_rows, name="min_row_check"),
            strict='filter'
        )

def validateInData(df, min_rows=200):
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(df).__name__}")

    return get_nasdaq_schema(min_rows).validate(df)
    

validated_data= validateInData(df)

def extract_columns(validated_data):
    """
    #Step 1: Extract two columns: Symbol & Name first.
    """
    required_cols = [DATA_COLS['ticker'], DATA_COLS['name'], DATA_COLS['valuations']]
    extracted_columns=validated_data[required_cols].copy()
    
    logger.info(f"Extraction Complete:{extracted_columns.shape[1]} columns retained")
    logger.info(f"Extraction Complete. Retained: {', '.join(extracted_columns.columns)}")

    return extracted_columns

def normalize_names(final_three_columns):
    copy_three_columns=final_three_columns.copy()

    #Step 2: Keeping the column lowercase and stripping
    copy_three_columns[INTERNAL_COLS['clean_name']]=copy_three_columns[DATA_COLS['name']].str.casefold().str.strip()

    #Step 3: Define noise patterns
    
    #Step 4: Apply regex for cleaning
    def clean_company_name(name, patterns, replacements):
        if not name:
            return ''
        
        #Basic standerdization

        cleaned=name.casefold().strip()
        for pattern in patterns:
            import re
            cleaned = re.sub(pattern, '', cleaned).strip()
        
        for old, new in replacements.items():
            cleaned =re.sub(old, new, cleaned).strip()
        return cleaned
    copy_three_columns[INTERNAL_COLS['clean_name']]=copy_three_columns[INTERNAL_COLS['clean_name']].apply(
        lambda x:clean_company_name(x, CLEANING_PATTERNS,CLEANING_REPLACEMENTS))

    return copy_three_columns

final_three_columns=extract_columns(validated_data)

normalized_df=normalize_names(final_three_columns)

logger.info(f"Normalization Complete: 'Name_clean' column method")
#print(normalized_df[['Name','Name_clean']].head())

def build_master_list(normalized_df):
    """
    This function defines our "Source of Truth."
    It identifies the primary ticker for every company to use as the match target.
    """

    #Step 6: Sort by Marketcap and symbol
    # We sort Market Cap DESCENDING so the most valuable version (Common Stock) is on top
    sort_normalized_df=normalized_df.sort_values(by=[DATA_COLS['ticker'], DATA_COLS['valuations']],ascending=[True, False])

    #Step 7: Drop by duplicates Symbol to ensure I have one record per ticker
    master_list= sort_normalized_df.drop_duplicates(subset=[DATA_COLS['ticker']]).copy()

    #Step 8:Filter for "Clean" Symbols only
    #Remove symbol with slashes. dashes, or warrants leaving primary listing  for the Master Reference list
    master_list=master_list[~master_list[DATA_COLS['ticker']].str.contains(SYMBOL_EXCLUSION_REGEX, regex=True)]

    logger.info(f"Master List Built: {len(master_list)} primary records identified.")
    
    return master_list

master_reference=build_master_list(normalized_df)
print(master_reference[[DATA_COLS['ticker'],INTERNAL_COLS['clean_name'],DATA_COLS['valuations']]].head(10))

def match_and_categorize(normalized_df,master_reference):
    """
    Matches messy names to the Master List and assigns Trust Levels immediately.
    """
    #Step 9: Prepare the search target from our Master list
    choices = master_reference[INTERNAL_COLS['clean_name']].tolist()

    #Step 10: Define the internal matching logic
    def get_single_match(messy_name):
        if not messy_name:
            return pd.Series([None, 0,"Red: Empty"])
        
        #One-shot fuzzy search
        result =process.extractOne(
            messy_name,
            choices, 
            scorer=fuzz.token_set_ratio
        )
        best_match, score, index = result 

        #Assign Trust Category
        if score >= THRESHOLDS['green']:
            category = LABELS['verified']
        elif score >= THRESHOLDS['yellow']:
            category = LABELS['review']
        else:
            category = LABELS['unknown']

        return pd.Series([best_match, score, category])
    #Step 11: Applying to the dataframe
    normalized_df[['match_name','match_score', INTERNAL_COLS['trust']]] =normalized_df[INTERNAL_COLS['clean_name']].apply(get_single_match)

    #Step 12: Merge the data
    normalized_df =normalized_df.merge(
        master_reference[['Name_clean', DATA_COLS['ticker'], DATA_COLS['valuations']]],
        left_on='match_name',
        right_on=INTERNAL_COLS['clean_name'],
        how='left',
        suffixes=('','_master')
    )

    logger.info("Matching and Categorization Complete")
    return normalized_df

final_categorized_df=match_and_categorize(normalized_df,master_reference)
logger.info(final_categorized_df.head())
logger.info(final_categorized_df.duplicated(subset=[DATA_COLS['ticker']]).sum())

def get_top_300(final_categorized_df):
    """
    Final Filtering and Ranking.
    Uses 'max' instead of 'sum' to avoid double-counting share classes.
    """

    #Step 13: Keep only Verified (Green) copy
    df_green=final_categorized_df[final_categorized_df['trust_level']=='Green: Verified'].copy()

    #Step 14:Group by the Match Name to handle duplicates like GOOG/GOOGL
    # We take the 'first' Symbol and the 'max' Market Cap

    top_groups = df_green.groupby('match_name').agg(
        {'Symbol_master':'first',
         'Market Cap_master':MARKET_CAP_AGG}
    ).reset_index()

    #Step 15: Rename & sort
    top_groups.columns=[DATA_COLS['name'], DATA_COLS['ticker'], DATA_COLS['valuations']]
    top_groups=top_groups.sort_values(by=DATA_COLS['valuations'], ascending=False)

    #Step 16: Trim to Top 300
    final_300= top_groups.head(300)

    logger.info(f"Top 300 list generated. Largest: {final_300.iloc[0][DATA_COLS['name']]}")
    print(final_300[DATA_COLS['ticker']].duplicated().sum())
    print(final_300[DATA_COLS['valuations']].min())
    return final_300
top_300=get_top_300(final_categorized_df)
#print(top_300.info())

def validate_top_300(top_300):
    #Confirm we have 300 rows  ONLY
    if len(top_300) !=300:
        raise ValueError(f" Expectedly exact rows, got {len(top_300)}")
    
    #Confirm the number of columns to be 3
    if top_300.shape[1]<3:
        raise ValueError(f" Top 300 has less than 3 columns")
    
    #Confirm the three required columns
    required_col =[DATA_COLS['ticker'], DATA_COLS['name'], DATA_COLS['valuations']]
    missing_col= []
    for col in required_col:
        if col not in top_300:
            missing_col.append(col)
    if missing_col:
        raise ValueError(f" The missing column(s) are {missing_col}")
    
    #Confirm market cap values to be float
    if  pd.api.types.is_float_dtype(top_300[DATA_COLS['valuations']]):
        logger.info(f"Market Cap Values are a float")

    #Confirm Minimums
    current_min=top_300[DATA_COLS['valuations']].min()
    logger.info(f"Validation Pass: 300 unique symbols. Minimum Cap: ${current_min:,.0f}")
    
    #Confirm symbols has no duplicates

    if top_300[DATA_COLS['ticker']].duplicated().any():
        dupes =top_300[top_300[DATA_COLS['ticker']].duplicated()][DATA_COLS['ticker']].tolist()
        raise ValueError(f" The duplicated symbol found:{dupes}")
    return top_300

validated_top_300= validate_top_300(top_300)
logger.info(f"Complete validation process of top 300 nasdaq public listed companies by Market Cap")

def pre_validate_with_yahoo(symbols):
    #Initialize  the ticker object with the FULL list
    t=Ticker(symbols)

    #Ask for the simple pieces of data(price-info)
    price_data=t.price

    #Ensure there's no errors with yahoo
    if not isinstance(price_data, dict):
        raise ValueError("Unexpected API response. Check the internet connection")

    #identify valids vs invalid symbols
    valid_symbols= []
    invaiid_symbols = []

    for s in symbols:
        #Yahooquery returns a dictionary of data found or string message error if not
        if isinstance(price_data.get(s), dict):
            valid_symbols.append(s)
        else:
            invaiid_symbols.append(s)

    #. The "270" Threshold
    count = len(valid_symbols)
    if count<270:
        raise ValueError(f"CRITICAL FAILURE: Only {count} symbols valid. Manual review required")
    logger.info(f"API check passed:{count} valid, {len(invaiid_symbols)} invalid.")
    return valid_symbols
#Preping the data
input_list=validated_top_300['Symbol'].tolist()
verified_symbols=pre_validate_with_yahoo(input_list)
#print(verified_symbols[0:50])
check_df= pd.DataFrame(verified_symbols[0:50],columns=['Verified_Ticker'])
#print(check_df)
logger.info(pd.Series(verified_symbols).describe())
logger.info(f"COMPLETE: Validation of symbols with Yahoo search is complete")

if __name__  == "__main__":
    from config.logging_config import setup_logging
    setup_logging()
    logger.info(f"Starting the Nasdaq Cleaning Process...")

    #Loading the data
    df=load_nasdaq_data()

    #Running the functions
    validated_data= validateInData(df)
    final_three_columns=extract_columns(validated_data)
    normalized_df=normalize_names(final_three_columns)
    master_reference=build_master_list(normalized_df)
    final_categorized_df=match_and_categorize(normalized_df,master_reference)
    top_300=get_top_300(final_categorized_df)
    validated_top_300= validate_top_300(top_300)

    logger.info(f"PIPELINE COMPLETE: Here are the top five rows")
    logger.info(validated_top_300.head(10))


    
    

