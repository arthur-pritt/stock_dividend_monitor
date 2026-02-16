import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo


    return


@app.cell
def _():
    import pandas as pd
    from config.settings import RAW_DATA_PATH
    from etl_pipeline.src.extract._clean_nasdaq_data import _cleaned_nasdaq_list

    return


@app.cell
def _():
    import pandas as pd
    from config.settings import RAW_DATA_PATH
    from etl_pipeline.src.extract._clean_nasdaq_data import _cleaned_nasdaq_list

    df = _cleaned_nasdaq_list()
    marketcap_sort_df= df.sort_values(by= 'Market Cap', ascending=False)
    top_110=marketcap_sort_df
    top_110.head(110)

    drop_column=top_110.drop(columns=['Last Sale', 'Net Change','% Change','IPO Year','Volume','Sector','Industry','Country'])
    drop_column.head(110)

    # Reset index to start from 1
    drop_column = drop_column.reset_index(drop=True)
    drop_column.index = drop_column.index + 1

    drop_column.head(110)

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
