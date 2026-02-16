import pandas as pd
import os

#Importing config files
from config.settings import RAW_DATA_PATH


def _load_nasdaq_data():
    """Loading the Nasdaq data list from config
    """

    try:
        #Load the data from config path
        df = pd.read_csv(RAW_DATA_PATH)

        #Validation: Checking if we actuall have the data
        if df.empty:
            print(f" ERROR: '{RAW_DATA_PATH} loaded but contain NO DATA ROWS")
            return None 
        print(f" Started cleaning the Nasdaq data list. INITIAL ROWS: {len(df)}")

    except FileNotFoundError: 
        print(f" File Not Found: {RAW_DATA_PATH}")
        print(f" Please check that the file exists in the config path")
        return None 
    
    except pd.errors.ParserError as e:
        print(f" CSV Parsing Error in '{RAW_DATA_PATH}")
        print(f" {e}")
        print(" The file maybe corrupted or not a valid csv")
        return None 
    
    except PermissionError:
        print(f" Permission denied :Cannot  read {RAW_DATA_PATH}")
        print(f" Check file permission.")
        return None 
    
    except Exception as e:
        print(f" Unexpected Error loading '{RAW_DATA_PATH}")
        print(f" {type(e).__name__}:{e}")
        return None 
    

def _cleaned_nasdaq_list():
    """
    Docstring for _cleaned_nasdaq_list
    """

    #Load data first and determine if is failed not not
    df =pd.read_csv(RAW_DATA_PATH)
    if df is None:
        return None 
    
    #Cleaning process begins
    print(f"Started the cleaning process.INITIAL ROWS: {len(df)}")


    #Inspecting/validating required columns
    required_columns = ['Symbol','Name','Market Cap','Last Sale', 'Net Change','% Change','IPO Year','Volume','Sector','Industry','Country' ]
    missing_columns =[]

    for col in required_columns:
        #checking if the column is not in required columns
        if col not in df.columns:
            missing_columns.append(col)

    if missing_columns:
        print(f"ERROR: Missing required columns {missing_columns}")
        print(f" Available columns: {list(df.columns)}")
        return None 
    

    #Sorting columns by market capitalization from the highest to smallest
    try:
        sorting_marketcap = df.sort_values(by= 'Market Cap', ascending=False)
    except Exception as e:
        print("ERROR: Validating by Market cap: {e}")
        return None 
    
    top_110= sorting_marketcap.head(110)

    #Warn if fewer than expected.
    if len(top_110)<110:
        print(f"WARNING: ONLY {len(top_110)} rows available (expected 110)")


    #Dropping unnessary columns
    drop_columns= top_110.drop(columns=['Last Sale', 'Net Change','% Change','IPO Year','Volume','Sector','Industry','Country'])

    #Reseting index. Will use symbol as the primary key
    clean_nasdaq_data= drop_columns.reset_index(drop=True)
    clean_nasdaq_data.index = clean_nasdaq_data.index + 1

    #Cleaning and sorting completed
    print(f"COMPLETED. Final rows:{len(clean_nasdaq_data)}")

    return clean_nasdaq_data

if __name__  == "__main__":
    cleaned_nasdaq_data = _cleaned_nasdaq_list()
    print(cleaned_nasdaq_data.head(50))
    

