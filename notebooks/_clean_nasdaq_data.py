import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo
    import pandas as pd
    import regex
    from rapidfuzz import process

    return pd, process


@app.cell
def _():
    #Import config files
    from config.settings import RAW_DATA_PATH
    from config.logging_config import get_logger


    return (RAW_DATA_PATH,)


@app.cell
def _(RAW_DATA_PATH, pd):
    def validateInData():
        df= pd.read_csv(RAW_DATA_PATH)
        print(df.info())
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Not a pandas DataFrame. It is a {df}")
    
        if df.shape[1]<=3 or df.shape[0]<=200:
            raise ValueError(f"Has less than 3 columns and 200 rows")
    
        required_col=["Symbol","Name","Market Cap"]
        missing_col = []
        for col in required_col:
            if col not in df.columns:
                missing_col.append(col)
    
        if missing_col:
            raise ValueError(f"Missing column: {missing_col}")
        return df

    validated_data=validateInData()
    print(validated_data.head())
    return (validateInData,)


@app.cell
def _(pd, validateInData):
    def check_unique_data():
        df=validateInData()
        return pd.Series(df['Symbol'].unique())
    unique_data=check_unique_data()
    print(unique_data[0:50])
    print(type(unique_data))
    
    return (check_unique_data,)


@app.cell
def _(check_unique_data, process):
    def similar_name_groups():
        df=check_unique_data()
        symbols=df.tolist()
        auto_group= {}
        for name in symbols:
            if auto_group:
                match, score,_ =process.extractOne(name,list(auto_group.keys()))
                if score > 80:
                    auto_group[match].append(name)
                else:
                    auto_group[name]=[name]
            else:
                auto_group[name]=[name]
        clean_ref_list = list(auto_group.keys())
        return clean_ref_list
    ref_list = similar_name_groups()
    print(ref_list)
    print(len(ref_list))

    return (similar_name_groups,)


@app.cell
def _(check_unique_data, pd, process, similar_name_groups):
    def get_best_match(messy_names,clean_single_names):
    
    
        clean_match = messy_names.apply(lambda x:pd.Series(process.extractOne(x,clean_single_names)[:2]))
        clean_match.columns=["Best_match", "score"]
        return clean_match
    mess_names =check_unique_data()
    clean_names =similar_name_groups()
    clean=get_best_match(mess_names, clean_names)
    print(clean[0:50])
    
    return


if __name__ == "__main__":
    app.run()
