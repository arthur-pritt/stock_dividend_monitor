import os
import  pandas as pd
from typing import Optional


#importing config files
from config.settings import RAW_DATA_PATH

def load_nasdaq_data()-> Optional[pd.DataFrame]:
    """Loading nasdaq csv list from data folder
       
    Returns:
     Optional[pd.DataFrame]= Dataframe containing Nasdaq data if successful or
                                 None if the file is missing, empty, or unreadable.
    """

    #accessing the nasdaq csv path
    nasdaq_csv_path = RAW_DATA_PATH
    print(f"Loading from {nasdaq_csv_path}")
    print(f"File exists at {nasdaq_csv_path.exists()}")

    #loading the data
    try:
        df = pd.read_csv(nasdaq_csv_path)
        print(f"SUCCESS: Data has been loaded {len(df)}")
        return df
    except FileNotFoundError:
        print(f"Error: File not found at {nasdaq_csv_path}")
        print(f" Ensure the file exists at data/raw/ folder")
        return None 
    except pd.errors.EmptyDataError:
        print("Error: The csv file is empty")
        return None 
    except Exception as e:
        print(f"Unexpected error loading data:{type(e).__name__}: {e}")
        return None 
#Usage with type hints 

if __name__ == "__main__":
    df: Optional[pd.DataFrame] = load_nasdaq_data()
    if df is not None:
        print(df.head())
    


