import pandas as pd 
import unittest
import pytest
import pandera.pandas as pa
from pandera.errors import SchemaError
from unittest.mock import patch, Mock, call
from pandera import Column,check,DataFrameSchema

#importing modules
from etl_pipeline.src.extract._backfill import validate_tickers,validate_data_out
from config.settings import DATA_COLS

class Testvalidate_tickers(unittest.TestCase):
    """
    Defining, validating and testing tickers in the schema
    """

    def test_validate_tickers(self):
        """
        Test that a valid dataframe passess schema and pandera."""
        #Creating a realistic dataframe or a fake one
        df = pd.DataFrame({
        DATA_COLS['ticker']: ['AAPL'] * 200,
        DATA_COLS['name']: ['Apple Inc'] * 200,
        DATA_COLS['valuations']: [150.0] * 200
        })
        
        validated = validate_tickers(df)
        self.assertEqual(len(validated), 200)

    def test_too_few_rows(self):
        df_small=pd.DataFrame({
            DATA_COLS['ticker']:['AAPL'],
            DATA_COLS['name']:['Apple Inc'],
            DATA_COLS['valuations']:[150]
        })
        #raising errors when the schema fails
        with self.assertRaises(pa.errors.SchemaError):
            validate_tickers(df_small)

    def test_non_dataframe(self):
        """
        Test that non-dataframe raises TypeError"""
        with self.assertRaises(TypeError):
            validate_tickers([1,2,50]) #putting a list
    
    def test_missing_columns(self):
        """
        Test missing required columns
        """
        df_bad=pd.DataFrame({
            "Symbol":['AAPL'],
            "Name":['apple inc']
            #missing valuation columns
        })
        with self.assertRaises(pa.errors.SchemaError):
            validate_tickers(df_bad)
class TestValidate_date_out(unittest.TestCase):
    def test_none_input(self):
        with self.assertRaises(ValueError):
            validate_data_out(None)
    
    def test_not_dataframe_input(self):
        df=[1,2,3]

        with self.assertRaises(TypeError):
            validate_data_out(df)
            
        
    def test_empty_dataframe(self):
        df=pd.DataFrame()
        with self.assertRaises(ValueError):
            validate_data_out(df)
    
    def test_too_few_rows(self):
        df=pd.DataFrame({
            "symbol": ["AAPL"], "date": ["2024-01-01"], "adjclose": [100.0],
            "volume": [1000], "coverage_pct": [0.95], "is_flagged": [False],
            "actual_days": [252]
        })
        with self.assertRaises(ValueError):
            validate_data_out(df)
    
    def test_missing_columns(self):
        df=pd.DataFrame({
            "symbol": ["AAPL"]*7000,
            "date": ["2024-01-01"]*7000,
            "adjclose": [100.0] * 7000,
            "volume": [1000] * 7000,
            "coverage_pct": [0.95] * 7000,
            "is_flagged": [False] * 7000,
            "actual_days": [252] * 7000,
        })

        df = df.drop(columns=["volume"])

        with self.assertRaises(SchemaError):
            validate_data_out(df)





    

