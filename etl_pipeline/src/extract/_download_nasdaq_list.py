import os
import  pandas as pd

def load_nasdaq_data():
    "Loading nasdaq list from data folder"

    #project script folder
    script_dir = os.path.dirname(os.path.abspath(__file__))

    #project root folder
    project_root = os.path.join(script_dir, '..','..','..')

    #accessing the nasdaq csv path
    nasdaq_csv_path = os.path.join(project_root, 'data', 'raw', 'nasdaq_100_list.csv')

    #loading the data
    df = pd.read_csv(nasdaq_csv_path)
    return df
    

df = load_nasdaq_data()
df 

