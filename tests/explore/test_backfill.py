import pandas as pd 
import unittest
import pandera.pandas as pa
from pandera.errors import SchemaError
from unittest.mock import patch, Mock, call
from pandera import Column,check,DataFrameSchema

#importing modules
from etl_pipeline.src.extract._backfill import validate_tickers
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
