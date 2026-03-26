import pandas as pd
import re
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
    extract_symbol_name= df[['Symbol', 'Name','Market Cap']].copy()
    #print(extract_symbol_name)

    #Step 2: Preserve the original
    extract_symbol_name['Name_raw'] = extract_symbol_name['Name']

    #Step 3:Normalize the Name column before matching and stripping away the financial naming suffix
    extract_symbol_name['Name_clean'] =extract_symbol_name['Name'].str.casefold()
    
    #Step 4: Remove every character that comes after a comma, common stock, ordinary shares and etc.
    #extract_symbol_name['Name_clean'] = (extract_symbol_name['Name_clean'].str.replace(r'\..*$', '' ,regex=True).str.strip())
    #extract_symbol_name['Name_clean'] = (extract_symbol_name['Name_clean'].str.replace(r'\b\d+\.?\d*%?\b', '', regex=True).str.strip())
    #extract_symbol_name['Name_clean'] = (extract_symbol_name['Name_clean'].str.replace(r'\b(corp|corporation)\b.*$', r'\1', regex=True))
    #extract_symbol_name['Name_clean'] = (extract_symbol_name['Name_clean'].str.replace(r'\b(common stock|ordinary shares?|common shares?)\b','',regex=True)
    #                                     .str.replace(r'\s+', ' ',regex=True)
    #                                     .str.strip())
    
    patterns=[
        r',.*$',
        r'\..*$', 
        r'\b(corporation|corp)\b.*$',
        r'\b(common stock|ordinary shares?|common shares?)\b',
        r'\b\d+\.?\d*%?\b', 

    ]
    def clean_company_name(name):
        cleaned=name
        for pattern in patterns:
            if pattern == r'\b(corporation|corp)\b.*$':
                cleaned = re.sub(pattern, r'\1', cleaned)  
            else:
                cleaned=re.sub(pattern,'',cleaned)
        cleaned= re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned
    
    extract_symbol_name['Name_clean']=extract_symbol_name['Name_clean'].apply(clean_company_name)
    print(extract_symbol_name[0:50])
    print(extract_symbol_name[50:100])
    print(extract_symbol_name[100:150])
    print(extract_symbol_name[150:200])
    print(extract_symbol_name[200:250])
    print(extract_symbol_name.info())

    return extract_symbol_name
    #
    # Step 5:
    
    suffix_to_remove=[items.strip().title() for items in suffix_to_remove]
    def clean_company_name(name):
        cleaned= name.title()
        for suffix in suffix_to_remove:
            cleaned=cleaned.replace(suffix,"")
        cleaned= " ".join(cleaned.split())
        cleaned=cleaned.strip('.,')
        return cleaned
    
    extract_symbol_name=extract_symbol_name.copy()
    print(type(extract_symbol_name))
    extract_symbol_name=extract_symbol_name.apply(clean_company_name)
    print(extract_symbol_name[0:50])
    print(extract_symbol_name[50:100])

    return extract_symbol_name




    

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
    
    

