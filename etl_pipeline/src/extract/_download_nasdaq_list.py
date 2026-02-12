import os
import  pandas as pd
from typing import Optional

def load_nasdaq_data()-> Optional[pd.DataFrame]:
    """Loading nasdaq csv list from data folder
       
    Returns:
     Optional[pd.DataFrame]= Dataframe containing Nasdaq data if successful or
                                 None if the file is missing, empty, or unreadable.
    """
    #project script folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"1. Script directory {script_dir}")

    #project root folder
    project_root = os.path.join(script_dir, '..','..','..')
    print(f"2. Project root folder {project_root}")

    #accessing the nasdaq csv path
    nasdaq_csv_path = os.path.join(project_root, 'data', 'raw', 'nasdaq_100_list.csv')
    print(f"3. File location at {nasdaq_csv_path}")
    print(f"File exists at {os.path.exists(nasdaq_csv_path)}")

    #Check what's in the raw folder
    raw_folder = os.path.join(project_root, 'data', 'raw')
    if os.path.exists(raw_folder):
        print(f"5. Files in the raw folder: {os.listdir(raw_folder)}")
    else:
        print(f"5. ERROR: raw folder doesn't exists at {raw_folder}")

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
df: Optional[pd.DataFrame] = load_nasdaq_data()
if df is not None:
    print("\n" + "="*50)
    print(df.head())
else:
    print("Cannot proceed without data")


