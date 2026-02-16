import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import os

    return


@app.cell
def _():
    import pandas as pd
    from config.settings import RAW_DATA_PATH
    from etl_pipeline._clean_nasdaq_data import _cleaned_nasdaq_list  # Use etl_pipeline!

    df = _cleaned_nasdaq_list()
    df.head()
    return


if __name__ == "__main__":
    app.run()
