#Importing necessary libraries for testing
import pandas as pd 
import unittest
import logging
import pandera.pandas as pa
from pandera.errors import SchemaError
from unittest.mock import patch, Mock, call
from datetime import datetime 
import pandas_market_calendars as mcal 


#Importing necessary modules
from etl_pipeline.src.extract._fetch_current_stock_price import validate_tickers,count_nyse_trading_days
from config.settings import DATA_COLS

class TestCount_trading_days(unittest.TestCase):
    def test_startdate_notgreaterthan_enddate(self):
        """Test that function raises ValueError when start_date > end_date."""
        with self.assertRaises(ValueError):
            count_nyse_trading_days("2024-01-10", "2024-01-01")
    
    def test_normal_date_range_returns_correct_count(self):
        result, dates=count_nyse_trading_days("2020-01-01","2020-01-10")
        self.assertIsInstance(result,int)
        self.assertGreater(result,0)
        self.assertIsInstance(dates, pd.DatetimeIndex)


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
        tickers= [f"TICK{i:04d}" for i in range(num_rows)]
        names= [f"COMPANY{i:04d} inc " for i in range(num_rows)]
        return pd.DataFrame({
            DATA_COLS['ticker']:tickers,
            DATA_COLS['name']:names,
            DATA_COLS['valuations']:[150.0 + (i * 10) for i in range(num_rows)]
        })
        

    def test_valid_dataframe(self):
        df= self.create_df(num_rows=300)
        results=validate_tickers(df)
        assert isinstance(results, pd.DataFrame)
        self.assertEqual(results.shape[0], 300)
        self.assertEqual(results[DATA_COLS["ticker"]].nunique(), 300)   # all symbols unique
        self.assertTrue(results[DATA_COLS["ticker"]].str.isupper().all())  # tickers are uppercase
        self.assertTrue(pd.api.types.is_float_dtype(results[DATA_COLS["valuations"]]))

    #Row count tests

    def test_250_300(self):
        df=self.create_df(num_rows=275) #number between 250 to 300

        results=validate_tickers(df)
        self.assertIsInstance(results, pd.DataFrame)
        self.assertEqual(results.shape[0], 275)

    def test_110_249(self):
        df=self.create_df(num_rows=180) #number here is 180
        
        with patch('logging.Logger.warning') as mock_warning:
            results=validate_tickers(df)

            #check that the functions returns a dataframe
            self.assertIsInstance(results, pd.DataFrame)
            self.assertEqual(results.shape[0],180)

            #check that a warning was logged
            mock_warning.assert_called_once()

            #warning text contains expected messages
            args, kwargs=mock_warning.call_args
            self.assertIn("below the ideal range",args[0].lower())
    
    def test_below_110(self):
        df=self.create_df(num_rows=80)

        with self.assertRaises(ValueError) as context:
            validate_tickers(df)

            self.assertin("110",str(context.exception))
            self.assertIn("80", str(context.exception))

    def test_emptystr_ticker(self):
        df=self.create_df(num_rows=150)

        #set one row to have empty string ticker and the othe whitespace
        df.loc[0, DATA_COLS['ticker']]="" #empty string
        df.loc[1, DATA_COLS['ticker']]=" " #whitespace

        with self.assertRaises(Exception) as context:   #Pandera raises a schema error
            validate_tickers(df)

        error_msg= str(context.exception).lower()
        self.assertTrue("empty" in error_msg or "length" in error_msg or "whitespace" in error_msg)

    def test_null_tickers(self):
        df=self.create_df(num_rows=150)
        df.loc[0, DATA_COLS['ticker']]= None 

        with self.assertRaises(Exception) as context:
            validate_tickers(df)

        error_msg= str(context.exception).lower()
        self.assertIn("null", error_msg)

    def test_duplicate_tickers(self):
        df=self.create_df(num_rows=150)
        df.loc[0, DATA_COLS['ticker']]='AAPL'
        df.loc[1, DATA_COLS['ticker']]='AAPL'

        with self.assertRaises(Exception) as context:
            validate_tickers(df)

        error_msg= str(context.exception).lower()
        self.assertIn("duplicate", error_msg)

    def test_null_marketcap(self):
        df=self.create_df(num_rows=150)

        #set marketcap values to be null
        df.loc[0:4,DATA_COLS["valuations"]]= None #first 5 rows to be None

        results= validate_tickers(df)

        self.assertIsInstance(results, pd.DataFrame)
        self.assertEqual(results.shape[0],145) #150-5dropped rows
        self.assertFalse(results[DATA_COLS["valuations"]].isna().any()) #No more Nans

    def test_missing_column(self):
        df=self.create_df(num_rows=150)

        #Drop the symbol column
        df_missing=df.drop(columns=DATA_COLS["ticker"])

        with self.assertRaises(Exception) as context:
            validate_tickers(df_missing)

        error_msg=str(context.exception).lower()
        self.assertTrue(any(word in error_msg for word in ["missing", "column", "schema"]))

    def test_uppercase_conversion(self):
        df=self.create_df(num_rows=150)

        #make lowercase tickers
        df.loc[0:49, DATA_COLS["ticker"]] = df.loc[0:49, DATA_COLS["ticker"]].str.lower()

        results=validate_tickers(df)
        self.assertTrue(results[DATA_COLS["ticker"]].str.isupper().all(),
                        "ALL SYMBOLS/TICKERS MUST BE UPPERCASE")



        



    