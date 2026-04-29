#Importing necessary libraries for testing
import pandas as pd 
import unittest
import pandera.pandas as pa
from pandera.errors import SchemaError
from unittest.mock import patch, Mock, call


#Importing necessary modules
from etl_pipeline.src.extract._fetch_current_stock_price import validate_tickers
from config.settings import DATA_COLS

class TestValidate_tickers(unittest.TestCase):
    def test_none_input(self):
        with self.assertRaises(ValueError):
            validate_tickers(None)
    
    def test_not_dataframe(self):
        df=[1,2,3]
        with self.assertRaises(TypeError):
            validate_tickers(df)
    
    def test_empty_dataframe(self):
        df=pd.DataFrame()
        with self.assertRaises(ValueError):
            validate_tickers(df)
        
    def create_df(self, num_rows=300):
        return pd.DataFrame({
            DATA_COLS['ticker']:['AAPL']*num_rows,
            DATA_COLS['name']:['Apple Inc']*num_rows,
            DATA_COLS['valuations']:['150.0']*num_rows
        })
        

    def test_valid_dataframe(self):
        df= self.create_df(num_rows=300)
        results=validate_tickers(df)
        assert isinstance(results, pd.DataFrame)
        



    