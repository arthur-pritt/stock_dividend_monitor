import pandas as pd
import regex
from rapidfuzz import process


#Importing config files
from config.settings import RAW_DATA_PATH
from config.logging_config import get_logger 

#Getting the logger for this module
logger = get_logger(__name__)


def validateInData():
    df = pd.read_csv(RAW_DATA_PATH)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Not a pandas dataframe. It is a {len(df)}")
    if df.shape[1] <=3 or df.shape[0] <=200:
        raise ValueError("data has less than 3 columns and 200 rows")
    
    required_col = ["Symbol", "Name", "Market Cap"]
    missing_col = []

    for col in required_col:
        if col not in df.columns:
            missing_col.append(col)

    if missing_col:
        raise ValueError(f"The missing column(s) is :{missing_col}")
    return df 

validated_data= validateInData()

def find_unique_symbol():
    df = validateInData()
    return pd.Series(df['Symbol'].unique())
unique_symbol=find_unique_symbol()
print(unique_symbol[0:50])


def grouping_similar_symbols():
    df=find_unique_symbol()
    symbols=df.tolist() 
    gen_group_symbols={}
    for name in symbols:
        if  gen_group_symbols:
            match, score, _ =process.extractOne(name, list(gen_group_symbols.keys()))
            if score >80:
                gen_group_symbols[match].append(name)
            else:
                gen_group_symbols[name]= [name]
        else:
            gen_group_symbols[name]= [name]

    symbol_ref_list=list(gen_group_symbols.keys())
    return symbol_ref_list
reference_list =grouping_similar_symbols()


def best_symbol_match():
    messy_symbols =pd.Series(find_unique_symbol())
    symbol_group_name = grouping_similar_symbols()

    clean_symbols =messy_symbols.apply(lambda x:pd.Series(process.extractOne(x, symbol_group_name)[:2]))
    clean_symbols.columns=["Best_match","score"]

    clean_symbols.insert(0,"messy_symbols",messy_symbols.values)
    return  clean_symbols
clean_match=best_symbol_match()
print(clean_match[0:50])
print(clean_match[50:100])

def group_best_match():
    df = best_symbol_match()
    sort_best_symbol=df.sort_values("score", ascending=False )

    df_group=(sort_best_symbol.groupby("Best_match").first().reset_index())
    return df_group 
symbol_names=group_best_match()
print(symbol_names)




#if __name__  == "__main__":
 #   from config.logging_config import setup_logging
  #  setup_logging()
   # cleaned_nasdaq_data = _cleaned_nasdaq_list()
    #print(cleaned_nasdaq_data.iloc[0:50])
    #print(cleaned_nasdaq_data.iloc[50:110])
    
    

