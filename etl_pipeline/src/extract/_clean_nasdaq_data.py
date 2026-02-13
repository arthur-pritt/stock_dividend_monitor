import pandas as pd
import os

#Importing config files
from config.settings import RAW_DATA_PATH

def _cleaned_nasdaq_list():
    """
    Docstring for _cleaned_nasdaq_list
    """

    #Load the data from the config path
    df = pd.read_csv(RAW_DATA_PATH)
    print(f"Started Cleaning the Nasdaq data list. INITIAL ROWS:{len(df)}")

    #Cleaning Complete
    print(f"COMPLETE.Final rows:{len(df)}")

    #Sorting columns by market capitalization from the highest to smallest
    sorting_marketcap = df.sort_values(by= 'Market Cap', ascending=False)
    top_110= sorting_marketcap.head(110)

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
    

