import pytest
import pathlib
import pandas as pd 

from etl_pipeline.src.transform._split_dividend_status import split_dividend_status
from config.settings import(
    CLASSIFICATION_FILEPATH
)


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


def test_split_dividend_status_leaking_path():
    """
    Testing if the dividend status column has 5% null values."""
    pass

def test_split_dividend_status_poison_pill():
    """
    Testing if the dividen status column has 15% null values."""

    pass