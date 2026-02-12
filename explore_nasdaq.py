import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import os




    return os, pd


@app.cell
def _(os, pd):
    def load_nasdaq_data():
        script_dir=os.path.dirname(os.path.abspath(__file__))
        project_root=os.path.join(script_dir,'..')
        nasdaq_csv_path=os.path.join(project_root,'data', 'raw','nasdaq_100_list.csv')
        df=pd.read_csv(nasdaq_csv_path)
        return df

    df = load_nasdaq_data()
    df
    return


if __name__ == "__main__":
    app.run()
