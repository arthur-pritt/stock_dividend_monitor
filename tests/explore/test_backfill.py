import pandas as pd 
import unittest
import pytest
import pandera.pandas as pa
from pandera.errors import SchemaError
from unittest.mock import patch, Mock, call
from pandera import Column,check,DataFrameSchema

#importing modules
from etl_pipeline.src.extract._backfill import validate_tickers,validate_data_out,clean_and_validate
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
class TestClean_and_validate(unittest.TestCase):
    def create_valid_df(self):
        return pd.DataFrame({
            'symbol':['AAPL']*7000,
            'date':pd.to_datetime(["2024-01-01"]*7000).tz_localize('UTC'),
            'adjclose':[100.0]*7000,
            'volume':[1000.0]*7000,
            'coverage_pct':[0.95]*7000,
            'is_flagged':[False]*7000,
            'actual_days':[252]*7000
        })
    def  test_none_input(self):
        with self.assertRaises(ValueError):
            clean_and_validate(None)

    def test_not_dataframe(self):
        df=[1,2,3]
        with self.assertRaises(TypeError):
            clean_and_validate(df)

    def test_empty_dataframe(self):
        df=pd.DataFrame()
        with self.assertRaises(ValueError):
            clean_and_validate(df)
    def test_valid_dataframe(self):
        df=self.create_valid_df()
        result_df, _=clean_and_validate(df)
        assert isinstance(result_df, pd.DataFrame)
        assert result_df.shape[0] >= 6830
    
    
    def test_naming_standerdization(self):
        df=self.create_valid_df()
        df.columns = [
            "Symbol", "DATE", "adjClose", 
            "VOLUME", "Coverage Pct", "Is Flagged", "Actual Days"
        ]

        results_df, _=clean_and_validate(df)
        expected_cols=[
            'symbol', 'date', 'adjclose', 
            'volume', 'coverage_pct', 'is_flagged', 'actual_days'
        ]
        self.assertListEqual(list(results_df.columns), expected_cols)

    
    def test_date_coercion(self):
        df=self.create_valid_df()
        df['date'] = "2024-01-01"
        results_df, _=clean_and_validate(df)

        self.assertTrue(pd.api.types.is_datetime64_any_dtype(results_df['date']))
        self.assertIsNotNone(results_df['date'].dt.tz)

    
    def test_date_timezone_standerdization(self):
        df=self.create_valid_df()
        df['date']=pd.to_datetime("2024-01-01")

        results_df,_=clean_and_validate(df)

        self.assertEqual(str(results_df['date'].dt.tz), 'UTC')

    def test_date_integrity_rejects_garbage(self):
        df=self.create_valid_df()
        df['date'] = df['date'].astype(object)
        df.loc[0,'date']= "not-a-date"

        results_df,_=clean_and_validate(df)
        self.assertEqual(len(results_df),6999)


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

        with self.assertRaises(ValueError):
            validate_data_out(df)
    
    def test_valid_dataframe_passes(self):
        df = pd.DataFrame({
        "symbol": ["AAPL"]*7000,
        "date": pd.to_datetime(["2024-01-01"]*7000).tz_localize('UTC'),
        "adjclose": [100.0]*7000,
        "volume": [1000]*7000,
        "coverage_pct": [0.95]*7000,
        "is_flagged": [False]*7000,
        "actual_days": [252]*7000
        })
        result = validate_data_out(df)
        self.assertIsInstance(result, pd.DataFrame)





    

