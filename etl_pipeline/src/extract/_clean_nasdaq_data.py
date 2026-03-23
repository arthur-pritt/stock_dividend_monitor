import pandas as pd
import regex
from rapidfuzz import process,fuzz


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
#print(validated_data)


def find_unique_symbol(df):
    #Step 1: Extract two columns: Symbol & Name first.
    extract_symbol_name= df[['Symbol', 'Name']]

    #Step 2: Get full list of names
    symbol_names= extract_symbol_name['Name'].tolist()

    #Step 3:Text normalization: Turning the names into a lower list and stripping away spaces.
    symbol_names = [items.strip().lower() for items in symbol_names]
    print(type(symbol_names))

    #Step 4: Stripping away finacial naming convenctions
    symbol_naming_convenctions=["Warrant", "Common Stock","Class A Ordinary Share Common Stock","class a common stock",
                                    "Units","Rights","(The) Common Stock","Series D Cummulative Preferred Stock",
                                    "Series E Cummulative Redeemable Preferred Stock", "5.35% Global Notes Due 2066",
                                    "Variable Rate Series A Perpetual Preferred Stock", "Series F Fixed-Rate Preferred Stock",
                                    "fund shares of beneficial interest","income & opportunities fund ii shares of beneficial interest",
                                    "depositary shares each representing a 1/40th interest in a share of 6.50% series g non-cumulative perpetual preferred stock",
                                    "fifth third bancorp depositary shares each representing a 1/1000th ownership interest in a share of non-cumulative perpetual preferred stock series k",
                                    "common units representing limited partner interests"," simon property group 8 3/8% series j cumulative redeemable preferred stock",
                                    "i warrant", "perpetual fixed-to-floating rate non-cumulative preferred stock series h",
                                    "class a ordinary shares", " 6.375% series d cumulative redeemable preferred stock liquidation preference $25 per share",
                                    "4.25% subordinated debentures due 2060", "ordinary shares","8.125% notes due 2029","6.75% series c cumulative redeemable preferred shares of beneficial interest"]
    
    symbol_naming_convenctions=[items.strip().lower() for items in symbol_naming_convenctions]
    name_list=[]

    for names in symbol_names:
        if  unwantedNames in symbol_naming_convenctions:
            names=symbol_names.replace(unwantedNames, "")
        name_list.append(names.strip())
    symbol_names=name_list
    
    print(symbol_names[0:50])
    return symbol_names

    
    
    #Step 4:Finding  similar names and ratio
    names_list= []

    for name in symbol_names:
        name_matches= process.extract(name,
                                      symbol_names,
                                      scorer= fuzz.token_set_ratio,
                                      limit=5)
        for name_match, score, _ in name_matches:
            if name != name_match:
                names_list.append((name,name_match, score))
    
    #Create Dataframe
    name_similarity_score = pd.DataFrame(names_list, columns=['name_1', 'name_2', 'score'])
    print(name_similarity_score[0:50])
    print(name_similarity_score[50:100])
    print(name_similarity_score[100:150])
    print(name_similarity_score[150:200])
    return name_similarity_score
    
symbol_name=find_unique_symbol(df)
#print(unique_symbol[0:50])


def grouping_similar_symbols(df):

    print(type(df))
    
    symbols=df['Symbol'].tolist() 
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
reference_list =grouping_similar_symbols(df)


def best_symbol_match():
    messy_symbols =pd.Series(find_unique_symbol())
    symbol_group_name = grouping_similar_symbols()

    clean_symbols =messy_symbols.apply(lambda x:pd.Series(process.extractOne(x, symbol_group_name)[:2]))
    clean_symbols.columns=["Best_match","score"]

    clean_symbols.insert(0,"messy_symbols",messy_symbols.values)
    return  clean_symbols
clean_match=best_symbol_match()
#print(clean_match[0:50])
#print(clean_match[50:100])

def group_best_match():
    df = best_symbol_match()
    sort_best_symbol=df.sort_values("score", ascending=False )

    df_group=(sort_best_symbol.groupby("Best_match").first().reset_index())
    return df_group 
symbol_names=group_best_match()

def all_symbol_variants():
    result = group_best_match()
    df_groups_full=(result.sort_values("score", ascending=False)
                    .groupby('Best_match')
                    .agg(best_score=("score","max"),
                         top_match=("messy_symbols","first"),
                         all_variants=("messy_symbols", list),
                         count=("messy_symbols","count"))
                         .reset_index())
    return df_groups_full
summary=all_symbol_variants()
print(summary)





#if __name__  == "__main__":
 #   from config.logging_config import setup_logging
  #  setup_logging()
   # cleaned_nasdaq_data = _cleaned_nasdaq_list()
    #print(cleaned_nasdaq_data.iloc[0:50])
    #print(cleaned_nasdaq_data.iloc[50:110])
    
    

