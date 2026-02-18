import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import requests
    import pandas as pd
    import datetime
    import os
    from yahooquery import Ticker
    from pandas import json_normalize

    return Ticker, pd


@app.cell
def _(Ticker, pd):
    symbols = ["AAPL", "AMZN", "NFLX", "GOOG"]
    results = []
    for symbol in symbols:
        tickers = Ticker(symbol)
        tickers_data=tickers.history(period='3mo',interval='1d')
        tickers_data = tickers_data.reset_index()
        results.append(tickers_data)
    data = pd.concat(results, ignore_index=True)
    print(data)


    return


if __name__ == "__main__":
    app.run()
