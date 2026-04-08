import pandas as pd
from pathlib import Path
import sys
import unittest
import pandera.pandas as pa
from unittest.mock import patch, Mock, call
from pandera import Column, Check, DataFrameSchema
from pandera.errors import SchemaError

#Importing the module
from etl_pipeline.src.extract._clean_nasdaq_data import validateInData,normalize_names,pre_validate_with_yahoo
from config.settings import DATA_COLS, INTERNAL_COLS

class TestValidateInData(unittest.TestCase):
    """
    Begin by defining proper pandera schema.
    """
    

    def test_valid_dataframe(self):
        """
        Test that a valid dataframe passess schema and pandera."""
        #Creating a realistic dataframe or a fake one
        df = pd.DataFrame({
        DATA_COLS['ticker']: ['AAPL'] * 200,
        DATA_COLS['name']: ['Apple Inc'] * 200,
        DATA_COLS['valuations']: [150.0] * 200
    })
        # Just call the function. If it doesn't raise an error, it's valid.
        validated = validateInData(df, min_rows=200)
        self.assertEqual(len(validated), 200)

    def test_too_few_rows(self):
        df_small=pd.DataFrame({
            DATA_COLS['ticker']:['AAPL'],
            DATA_COLS['name']:['Apple Inc'],
            DATA_COLS['valuations']:[150]
        })
        #raising errors when the schema fails
        with self.assertRaises(pa.errors.SchemaError):
            validateInData(df_small, min_rows=200)

    def test_non_dataframe(self):
        """
        Test that non-dataframe raises TypeError"""
        with self.assertRaises(TypeError):
            validateInData([1,2,50]) #putting a list
    
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
            validateInData(df_bad)
class TestNameNormalization(unittest.TestCase):
    def test_clean_company_name_logic(self):
        """Test if noise like 'Inc.' and '(DE)' is stripped correctly."""
        # Arrange: Create a messy input
        df = pd.DataFrame({
            DATA_COLS['name']: ["Apple Inc.", "Microsoft Corp (DE)", "Alphabet  "]
        })
        
        # Act
        result = normalize_names(df)
        
        
        # Assert: Check the 'Name_clean' column (or whatever is in INTERNAL_COLS)
        clean_col = INTERNAL_COLS['clean_name']
        self.assertIn("apple", result[clean_col].iloc[0])
        
class TestTop300Logic(unittest.TestCase):
    def test_market_cap_aggregation(self):
        """Test that we take the MAX market cap for duplicate companies."""
        from etl_pipeline.src.extract._clean_nasdaq_data import get_top_300
        from config.settings import LABELS,DATA_COLS # Import the actual label
        
        # Arrange: Two entries for 'Google' with different caps
        df = pd.DataFrame({
            'match_name': ['google', 'google', 'apple'],
            'trust_level': [LABELS['verified']] * 3, 
            'Symbol_master': ['GOOG', 'GOOGL', 'AAPL'],
            'Market Cap_master': [2500.0, 1000.0, 3000.0]
        })
        
        # Act
        result = get_top_300(df)

        self.assertFalse(result.empty, "The result dataframe is empty! Check your trust_level labels.")
        
        # Assert: Google should exist once with the MAX value (2500)
        google_row = result[result[DATA_COLS['ticker']] == 'GOOG']
        self.assertEqual(len(google_row), 1, f"Expected 1 row for GOOG, found {len(google_row)}")
        self.assertEqual(google_row[DATA_COLS['valuations']].iloc[0], 2500.0)
class TestYahooValidation(unittest.TestCase):
    @patch('etl_pipeline.src.extract._clean_nasdaq_data.Ticker')
    def test_api_failure_threshold(self, mock_ticker):
        """Test that code raises ValueError if too many symbols fail."""
        # Arrange: Simulate Yahoo returning 'None' for all symbols
        instance = mock_ticker.return_value
        instance.price = {"AAPL": "No data found"} 
        
        # Act & Assert
        with self.assertRaises(ValueError) as cm:
            pre_validate_with_yahoo(["AAPL"])
        
        self.assertIn("CRITICAL FAILURE", str(cm.exception))


    

    
