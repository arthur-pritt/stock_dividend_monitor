import pytest
import pathlib
import pandas as pd 

from etl_pipeline.src.transform._split_dividend_status import split_dividend_status


def test_split_dividend_status_happy_path(tmp_path: pathlib.Path):
    """Testing if dividend status column has 0% nulls"""
    
    #1.Arrange: Creating the path string in the sandbox
    csv_path = tmp_path / "mock_data.csv"

    #2.Arrange: Building the actual dataframe
    mock_dict={
        'ticker': ['AAPL','MSFT'],
        'dividend_status':['dividend_paying','dividend_paying']
    }
    df_mock=pd.DataFrame(mock_dict)

    #3.Arrange: Save to the sandboxed path
    df_mock.to_csv(csv_path,index=False)

    #4.ACT: Pass the path
    df_dividend, df_non_dividend= split_dividend_status(csv_path)

    #5.ASSERT:Run type verification
    assert isinstance(df_dividend, pd.DataFrame)
    assert isinstance(df_non_dividend, pd.DataFrame)


    #5.Assert: Conservation of Mass verification (Row counts)
    assert len(df_dividend) == 2
    assert len(df_non_dividend)== 0


def test_split_dividend_status_leaking_path(tmp_path : pathlib.Path):
    """
    Testing if the dividend status column has 10% null values."""
    
    #1.Arrange: Creating the path string in the sandbox
    csv_ticker_path =tmp_path / "mock_data.csv"

    #2.Arrange: Creating the dataframe
    mock_dict = {
        'ticker':[
            'AAPL','NVDA','GOOG','NRG','MSFT','AMZN','TSLA','JPM','V','WMT'
        ],
        'dividend_status':[
            'dividend_paying','dividend_paying','dividend_paying','dividend_paying','dividend_paying','dividend_paying','dividend_paying', 'dividend_paying',
            'no_dividend_paying', None
        ]
    }

    mock_df = pd.DataFrame(mock_dict)

    #3.Arrange: Save to the path string sandbox
    mock_df.to_csv(csv_ticker_path, index=False)

    #4.ACT: Pass the Path
    df_dividend, df_non_dividend= split_dividend_status(csv_ticker_path)

    #5.Assert; Run type verification
    assert isinstance(df_dividend, pd.DataFrame)
    assert isinstance(df_non_dividend, pd.DataFrame)

    #6.Assert: Conservation of Mass Verification(Row Counts)
    assert len(df_dividend) == 8
    assert len(df_non_dividend) == 1

    #Verift file exists in the sandbox and has corrupted row
    corrupt_file_path = tmp_path / "corrupted_data.csv"
    assert corrupt_file_path.exists()

    df_corrupt = pd.read_csv(corrupt_file_path)
    assert len(df_corrupt)== 1


def test_split_dividend_status_poison_pill(tmp_path : pathlib.Path):
    """
    Testing if the dividen status column has 15% null values."""

    #1.Arrange: Creating the string path in the sandbox.
    csv_path = tmp_path / "mock_data.csv"

    #2.Arrange: Creating the real dataframe
    mock_dict = {
        'ticker':[
            'AAPL','NVDA','GOOG','NRG','MSFT','AMZN','TSLA','WMT','V', 'META','LLY','XOM','JNJ','MA','COST','MU',
            'BAC','ORCL','ABBV','HD'],
        'dividend_status':[
            'dividend_paying','dividend_paying', 'dividend_paying','dividend_paying','dividend_paying','dividend_paying','dividend_paying','dividend_paying',
            'dividend_paying','dividend_paying', 'dividend_paying', 'dividend_paying','dividend_paying', None, None, None,
            'dividend_paying', 'non_dividend_paying', 'non_dividend_paying', 'non_dividend_paying']
    }

    mock_df = pd.DataFrame(mock_dict)

    #3.Arrange: Saving the dataframe to csv path sandbox
    mock_df.to_csv(csv_path, index=False)

    #4.ACT: Pass the string path to the function
    with pytest.raises(ValueError):
        split_dividend_status(csv_path)

