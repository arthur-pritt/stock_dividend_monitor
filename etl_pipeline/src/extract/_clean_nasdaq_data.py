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
    #print(extract_symbol_name[0:50])
    #print(extract_symbol_name[50:100])

    #Step 2: Preserve the original
    extract_symbol_name['Name_raw'] = extract_symbol_name['Name']

    #Step 3:Normalize the Name column before matching and stripping away the financial naming suffix
    extract_symbol_name['Name_clean'] =extract_symbol_name['Name'].str.casefold()
    
    #Step 4: Remove every character that comes after a comma, common stock, ordinary shares and etc using regex pattern matching
    
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
    #Checking if Null values exist & empty string
    #print(extract_symbol_name.isna() |(extract_symbol_name['Name_clean'].str.strip()== ''))
    #print(extract_symbol_name[['Symbol','Market Cap','Name_clean']][200:250])
    #print(extract_symbol_name[100:150])
    #print(extract_symbol_name[150:200])
    #print(extract_symbol_name[200:250])
    #print(extract_symbol_name.info())
    
    # Step 5:Normalize the Name_clean column:"change corporation to corp", "incorporation to inc", & "Company to co"
    replacements ={
        r'\bcorporation\b':'corp',
        r'\bcompany\b' :'co',
        r'\bincorporation\b':'inc'
    }

    extract_symbol_name['Name_clean']= extract_symbol_name['Name_clean'].str.replace(replacements, regex=True)

    #Step 6: Remove null values and empty string in Marketcap
    #print(extract_symbol_name['Market Cap'].isna().sum())
    #print((extract_symbol_name['Market Cap'].astype(str).str.strip() == '').sum())

    null_values=extract_symbol_name['Market Cap'].notna()
    empty_string=extract_symbol_name['Market Cap'].astype(str).str.strip() !=''
    extract_symbol_name=extract_symbol_name[null_values & empty_string]

    #Step 7:Sort the data so that the most valuable companies come on 'Top'
    ## This ensures that 'Argan Inc' (Common) is seen before 'Argan Inc' (Warrant)
    extract_symbol_name = extract_symbol_name.sort_values(by=['Symbol','Market Cap'], ascending=[True, False])

    #Step 8: Create a master reference list and filter it the list
    #drop duplicates based on symbol to keep only the 'TOP' record for eacher Ticker
    ## We only want the Master List to contain "Common Stock" (no slashes or dots in symbols)
    master_list= extract_symbol_name.drop_duplicates(subset=['Symbol']).copy()
    master_list =master_list[~master_list['Symbol'].str.contains(r'[/-]|\.WS', regex=True)]

    #print(f"Original Records:{len(extract_symbol_name)}")
    #print(f"Master Reference Records:{len(master_list)}")

    #Step 9: Create a list of 'clean names' from the master list using 'Name_clean' column
    choices = master_list['Name_clean'].tolist()

    #Step 10: A function that finds the best match
    def find_best_match(messy_name):
        #if the name is empty, skipt it
        if not messy_name:
            return None, 0
        
        #Extract to find the best single match from the choices list and use the token_set_ratio scorer
        result=process.extractOne(messy_name, choices,scorer=fuzz.token_set_ratio)

        #result returns:(Matched_Name,Score,Index)
        return result[0], result[1]
    #Step 11: Applying to the original 4986 names and create two columns one for the 'Standard Name' and one for the 'Certainty Score'
    extract_symbol_name[['standardized_name','match_score',]]=extract_symbol_name['Name_clean'].apply(
        lambda x:pd.Series(find_best_match(x))
    )

    #Step 12 : Define the Trust Thresholds ---
    # These are the "Buckets" 
    HIGH_TRUST = 90
    MEDIUM_TRUST = 70

    #Step 13: Create the matching function

    def categorize_matches(messy_name, master_choices):
        """
        Find the best match and assign a trust category
        """

        if not messy_name:
            return pd.Series([None, 0,"Red: Empty"])
        # Find the single best match in our Master List
        # We use token_set_ratio because it handles 'Common Stock' noise best

        result =process.extractOne(
            messy_name,
            master_choices,
            scorer=fuzz.token_set_ratio
        )

        best_match, score, index= result

        # Assign the Category/Flag
        if score >= HIGH_TRUST:
            category = 'Green: Verified'
        elif score >= MEDIUM_TRUST:
            category = 'Yellow: Review Needed'
        else:
            category = 'Red: New/Unknown'
        return pd.Series([best_match, score, category])
    #Step 14:Prepare the Master List names as a list for faster searching and apply
    choices= master_list['Name_clean'].tolist()
    extract_symbol_name[['match_name','match_score','trust_level']]=extract_symbol_name['Name_clean'].apply(
        lambda x: categorize_matches(x,choices)
    )
    #Step 15: Mapping the  Real Symbols and Market cap from the Master list back to the original dataframe
    extract_symbol_name=extract_symbol_name.merge(
        master_list[['Name_clean', 'Symbol', 'Market Cap']],
        left_on='match_name',
        right_on='Name_clean',
        how='left',
        suffixes=('','_master')
    )
    print(f"Processing Complete You can now filter by trust_level")
    #Step 16: Keep only the HIGH_CONFIDENCE "green" matches for the final list
    extract_symbol_name_clean=extract_symbol_name[extract_symbol_name['trust_level']=='Green: Verified'].copy()

    #Step 17: Logging result for the checkpoint
    #print(f"---DATA CHECKPOINT---")
    #print(extract_symbol_name['trust_level'].value_counts())
    #print(f"Total Original Rows:{len(extract_symbol_name)}")
    #print(f"Verified Rows (Green):{len(extract_symbol_name_clean)}")
    #print(f"Discard Rows (Yellow/Red):{len(extract_symbol_name)-len(extract_symbol_name_clean)}")

    #Step 18: Grouping and Summing
    # Group by the 'Standardized Name' and 'Symbol' from the Master List
    # This ensures that all 'Argan' variations are summed into one total value
    top_groups=extract_symbol_name_clean.groupby(['Symbol_master','match_name']).agg(
        {'Market Cap_master':'sum'}
    ).reset_index()

    #Step 19: Sort by marketcap and trim
    top_groups.columns=['Symbol','Name','Market Cap']
    top_groups=top_groups.sort_values(by='Market Cap', ascending=False)
    final_200=top_groups.head(200)

    print(f"--- FINAL SUMMARY ---")
    print(f"Top 200 list created. Largest Company: {final_200.iloc[0]['Name']}")
   




    #print(extract_symbol_name[['Symbol', 'Market Cap', 'standardized_name']][0:50])
    #print(master_list[['Symbol','Name','Market Cap']][100:50])
    #print(master_list[['Symbol','Name','Market Cap']][150:200])


    return extract_symbol_name


    
    
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
    
    

