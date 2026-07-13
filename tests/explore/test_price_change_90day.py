import pytest 
import pathlib
import os 
import pandas as pd 
from datetime import datetime, timedelta

from etl_pipeline.src.transform.price_change_90day import price_change_calculation

def test_price_change_happy_path(tmp_path, monkeypatch):
    """
    Testing the happy path where everything is working well."""

    # 1. ARRANGE: Pipeline writes logs to an isolated tmp_path sandbox
    # setup_logging() writes logs into tmp_path/logs instead of the real project.
    # (e.g., use monkeypatch to switch working directory to tmp_path)
    monkeypatch.chdir(tmp_path)

    # 2.ARRANGE: Build clean mock INPUT DataFrames with known physical values
    # DataFrame A: classified_data ( with current dates & prices)
    # DataFrame B: historical_data ( with 91-day old historical dates & prices)
    # Hint: Setup the prices intentionally to hit your boundary values(50.0%, 49.9%,-20.0%,-19.9%)

    today = datetime(2026,7,7)

    classified_data = pd.DataFrame({
        'ticker':['AAPL', 'GOOG','MSFT', 'AMZN','TSLA','JPM','V','WMT','NRG','NVDA','C','KO'],
        'adj_close':[150.0, 149.9, 80.0, 80.1, 20.00, 400.0, 350.0, 150.0, 64.1, 150.0, 149.0, 80.0],
        'date':[today]*12
    })
    historical_data=pd.DataFrame({
        'ticker':['AAPL', 'GOOG','MSFT', 'AMZN','TSLA','JPM','V','WMT','NRG','NVDA','C','KO'],
        'adjclose':[100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0 ],
        'date':[
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91)
        ]

    })

    # 3. ACT: Passing the mock inputs into the real function directly
    result_df = price_change_calculation(classified_data, historical_data)

    # 4. ASSERT: Verify the Math Engine Outputs
    # Assert result_df shape is correct
    # Assert explicit boundary tags matching ('SKYROCKET','NORMAL','DROP')
    # Assert calendar delta matches expected days.



    assert result_df.shape == (12,10)
    assert 'watchlist_status' in result_df.columns #Every output must include watchlist column
    assert 'pct_change' in result_df.columns
    expected = [
        "SKYROCKET",
        "NORMAL",
        "NORMAL",
        "NORMAL",
        "SKYROCKET",
        "DROP",
        "DROP",
        "DROP",
        "SKYROCKET",
        "DROP",
        "SKYROCKET",
        "SKYROCKET",
        ]
    assert result_df["watchlist_status"].tolist() == expected
    assert result_df['pct_change'].between(-100,1000).all()


    # 5. ASSERT: Verify Data Quality Gate Side=effects
    # Assert that zero_tickers.csv, failed_tickers.csv, and dropped_tickers.csv DO NOT exist inside tmp_path

    zero_tickers_csv = tmp_path / "zero_tickers.csv"
    assert not zero_tickers_csv.exists()

    failed_tickers_csv= tmp_path / "failed_tickers.csv"
    assert not failed_tickers_csv.exists()

    dropped_tickers_csv = tmp_path / "dropped_tickers.csv"
    assert not dropped_tickers_csv.exists()

def test_price_change_leaky_path(tmp_path, monkeypatch):
    """
    Testing leaky path where some parts are not working well."""

    #1. ARRANGE: Pipeline writes logs to an isolated tmp_path sandbox
    #            setup_logging () writes logs into tmp_path sandbox
    monkeypatch.chdir(tmp_path)

    #2. ARRANGE: Build mock up dataframe with values.
    #Dataframe A: Classified_data (with current dates and prices)
    #Dataframe B: Historical_data (with 91-day old historical date & prices)
    #HINT: Make sure parts of adjclose in classified have 0 values and we have 35days in historical data.

    today = datetime(2026,7,7)

    classified_data= pd.DataFrame({
        'ticker': ['NVDA', 'GOOG','MSFT','AMZIN','TSLA','V','A','META','JPM','NRG','C','KO','DELL'],
        'adj_close':[0.0, 150.0, 100.0, 50.0, 149.0, 64.1, 400.0, 80.0, 80.1, 150.0, 64.1, 150.0, 300.0],
        'date': [today]*13
    })

    historical_data=pd.DataFrame({
        'ticker':['NVDA', 'GOOG','MSFT','AMZIN','TSLA','V','A','META','JPM','NRG','C','KO'],
        'adjclose':[100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
        'date':[
            today - timedelta(days=91),
            today - timedelta(days=45),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91)
            ]
    })

    #ACT: Passing the mock inputs into the real function directly
    result_df= price_change_calculation(classified_data, historical_data)

    #ASSERT: Verify the outputs
    assert result_df.shape == (10, 10)
    count= historical_data['date'].nunique()
    print(f"inspecting the count:{count}")
    assert count >=2

    # 5. ASSERT: Verify Data Quality Gate Side=effects
    # Assert that zero_tickers.csv, failed_tickers.csv, and dropped_tickers.csv exist inside tmp_path

    zero_tickers_csv = tmp_path / "zero_tickers.csv"
    assert  zero_tickers_csv.exists()

    failed_tickers_csv= tmp_path / "failed_tickers.csv"
    assert failed_tickers_csv.exists()

    dropped_tickers_csv = tmp_path / "dropped_tickers.csv"
    assert  dropped_tickers_csv.exists()

def test_price_change_poison_path(tmp_path, monkeypatch):
    """
    Testing where everything fails in the data quality gate."""

    #1. ARRANGE: Pipeline writes logs to an isolated tmp_path sandbox
    #            setup_logging () writes logs into tmp_path sandbox
    monkeypatch.chdir(tmp_path)

    #2. ARRANGE: Build mock up dataframe with values.
    #Dataframe A: Classified_data (with current dates and prices)
    #Dataframe B: Historical_data (with 91-day old historical date & prices)
    #HINT: Make sure parts of adjclose in classified have 0 values and we have 35days in historical data.

    today = datetime(2026,7,7)

    classified_data= pd.DataFrame({
        'ticker': ['NVDA', 'GOOG','MSFT','AMZIN','TSLA','V','A','META','JPM','NRG','C','KO'],
        'adj_close':[0.0, 0.0, 0.0, 0.0, 0.0, 64.1, 400.0, 80.0, 80.1, 150.0, 64.1, 150.0],
        'date': [today]*12
    })

    historical_data=pd.DataFrame({
        'ticker':['NVDA', 'GOOG','MSFT','AMZIN','TSLA','V','A','META','JPM','NRG','C','KO'],
        'adjclose':[100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
        'date':[
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91),
            today - timedelta(days=91)
            ]
    })

    #ACT & ASSERT: Passing the mock inputs into the real function directly and verify the outputs
    with pytest.raises(ValueError) as exc_info:
        price_change_calculation(classified_data, historical_data)
    
    assert f"Pipeline stopped: Zero price occurrence exceeds the 10% error budget." in str(exc_info.value)




