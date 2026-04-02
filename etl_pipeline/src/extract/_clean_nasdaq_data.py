import pandas as pd
import re
from rapidfuzz import process,fuzz
from yahooquery import Ticker 


#Importing config files
from config.settings import RAW_DATA_PATH
from config.logging_config import get_logger 
from _download_nasdaq_list import load_nasdaq_data

#Getting the logger for this module
logger = get_logger(__name__)

df=load_nasdaq_data()
def validateInData(df):
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Not a pandas dataframe. It is a {type(df).__name__}")
    if df.shape[1] <3 or df.shape[0] <200:
        raise ValueError("data has less than 3 columns and 200 rows")
    
    required_col = ["Symbol", "Name", "Market Cap"]
    missing_col = []

    for col in required_col:
        if col not in df.columns:
            missing_col.append(col)

    if missing_col:
        raise ValueError(f"The missing column(s) is :{missing_col}")
    return df 

validated_data= validateInData(df)

def extract_columns(validated_data):
    """
    #Step 1: Extract two columns: Symbol & Name first.
    """
    required_cols = ["Symbol", "Name", "Market Cap"]
    extracted_columns=validated_data[required_cols].copy()
    
    print(f"Extraction Complete:{extracted_columns.shape[1]} columns retained")
    print(f"Extraction Complete. Retained: {', '.join(extracted_columns.columns)}")

    return extracted_columns

final_three_columns=extract_columns(validated_data)
#print(final_three_columns.head())

def normalize_names(final_three_columns):
    copy_three_columns=final_three_columns.copy()

    #Step 2: Keeping the column lowercase and stripping
    copy_three_columns['Name_clean']=copy_three_columns['Name'].str.casefold().str.strip()

    #Step 3: Define noise patterns
    patterns=[
        r',.*$',
        r'\..*$', 
        r'\b(corporation|corp)\b.*$',
        r'\b(common stock|ordinary shares?|common shares?)\b',
        r'\b\d+\.?\d*%?\b', 
    ]

    #Step 4: Apply regex for cleaning
    def clean_company_name(name):
        cleaned=name
        for pattern in patterns:
            if pattern == r'\b(corporation|corp)\b.*$':
                cleaned = re.sub(pattern, r'\1', cleaned)  
            else:
                cleaned=re.sub(pattern,'',cleaned)
        cleaned= re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned
    copy_three_columns['Name_clean']=copy_three_columns['Name_clean'].apply(clean_company_name)

    #Step 5: Final Standardization
    replacements ={
        r'\bcorporation\b':'corp',
        r'\bcompany\b' :'co',
        r'\bincorporation\b':'inc'
    }

    copy_three_columns['Name_clean']=copy_three_columns['Name_clean'].str.replace(replacements, regex=True)
    return copy_three_columns
normalized_df=normalize_names(final_three_columns)

print(f"Normalization Complete: 'Name_clean' column method")
#print(normalized_df[['Name','Name_clean']].head())

def build_master_list(normalized_df):
    """
    This function defines our "Source of Truth."
    It identifies the primary ticker for every company to use as the match target.
    """

    #Step 6: Sort by Marketcap and symbol
    # We sort Market Cap DESCENDING so the most valuable version (Common Stock) is on top
    sort_normalized_df=normalized_df.sort_values(by=['Symbol','Market Cap'],ascending=[True, False])

    #Step 7: Drop by duplicates Symbol to ensure I have one record per ticker
    master_list= sort_normalized_df.drop_duplicates(subset=['Symbol']).copy()

    #Step 8:Filter for "Clean" Symbols only
    #Remove symbol with slashes. dashes, or warrants leaving primary listing  for the Master Reference list
    master_list=master_list[~master_list['Symbol'].str.contains(r'[/-]|\.WS', regex=True)]

    print(f"Master List Built: {len(master_list)} primary records identified.")
    
    return master_list

master_reference=build_master_list(normalized_df)
print(master_reference[['Symbol','Name_clean','Market Cap']].head(10))

def match_and_categorize(normalized_df,master_reference):
    """
    Matches messy names to the Master List and assigns Trust Levels immediately.
    """
    #Step 9: Prepare the search target from our Master list
    choices = master_reference['Name_clean'].tolist()

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
        if score >= 90:
            category = 'Green: Verified'
        elif score >= 70:
            category = 'Yellow: Review Needed'
        else:
            category = 'Red: New/Unkown'

        return pd.Series([best_match, score, category])
    #Step 11: Applying to the dataframe
    normalized_df[['match_name','match_score', 'trust_level']] =normalized_df['Name_clean'].apply(get_single_match)

    #Step 12: Merge the data
    normalized_df =normalized_df.merge(
        master_reference[['Name_clean','Symbol','Market Cap']],
        left_on='match_name',
        right_on='Name_clean',
        how='left',
        suffixes=('','_master')
    )

    print("Matching and Categorization Complete")
    return normalized_df

final_categorized_df=match_and_categorize(normalized_df,master_reference)
print(final_categorized_df.head())
print(final_categorized_df.duplicated(subset=['Symbol']).sum())

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
         'Market Cap_master':'max'}
    ).reset_index()

    #Step 15: Rename & sort
    top_groups.columns=['Name','Symbol','Market Cap']
    top_groups=top_groups.sort_values(by='Market Cap', ascending=False)

    #Step 16: Trim to Top 300
    final_300= top_groups.head(300)

    print(f"Top 300 list generated. Largest: {final_300.iloc[0]['Name']}")
    print(final_300['Symbol'].duplicated().sum())
    print(final_300['Market Cap'].min())
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
    required_col =['Symbol','Name', 'Market Cap']
    missing_col= []
    for col in required_col:
        if col not in top_300:
            missing_col.append(col)
    if missing_col:
        raise ValueError(f" The missing column(s) are {missing_col}")
    
    #Confirm market cap values to be float
    if not pd.api.types.is_float_dtype(top_300['Market Cap']):
        print(f"Market Cap Values are a float")

    #Confirm Minimums
    current_min=top_300['Market Cap'].min()
    print(f"Validation Pass: 300 unique symbols. Minimum Cap: ${current_min:,.0f}")
    
    #Confirm symbols has no duplicates

    if top_300['Symbol'].duplicated().any():
        dupes =top_300[top_300['Symbol'].duplicated()]['Symbol'].tolist()
        raise ValueError(f" The duplicated symbol found:{dupes}")
    return top_300

validated_top_300= validate_top_300(top_300)
print(f"Complete validation process of top 300 nasdaq public listed companies by Market Cap")

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
    print(f"API check passed:{count} valid, {len(invaiid_symbols)} invalid.")
    return valid_symbols
#Preping the data
input_list=validated_top_300['Symbol'].tolist()
verified_symbols=pre_validate_with_yahoo(input_list)
print(verified_symbols[0:50])
check_df= pd.DataFrame(verified_symbols[0:50],columns=['Verified_Ticker'])
print(check_df)
print(pd.Series(verified_symbols).describe())






#if __name__  == "__main__":
 #   from config.logging_config import setup_logging
  #  setup_logging()
   # cleaned_nasdaq_data = _cleaned_nasdaq_list()
    #print(cleaned_nasdaq_data.iloc[0:50])
    #print(cleaned_nasdaq_data.iloc[50:110])
    
    

