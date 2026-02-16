import pandas as pd
from pathlib import Path
import sys
from unittest.mock import patch, Mock, call

#Importing the module
from etl_pipeline.src.extract._clean_nasdaq_data import _cleaned_nasdaq_list



class TestCleanNasdaqDataHappyPath:
    """
    Testing the function when everything is working OK.
    """

    def test_returns_cleaned_dataframe_with_top_110_stock(self,mocker):
        #Arrange: Create fake csv data
        #This is what pd.read_csv(RAW_DATA_PATH)

        fake_data={'Symbol':[
            'NVDA', 'AAPL', 'GOOG', 'GOOGL', 'MSFT',
            'AMZN', 'META', 'AVGO', 'TSLA', 'BRK/A',
            'BRK/B', 'WMT', 'LLY'] + [f'STOCK{i}' for i in range(97)],
                   'Name': [
            'NVIDIA Corporation Common Stock',
            'Apple Inc. Common Stock',
            'Alphabet Inc. Class C Capital Stock',
            'Alphabet Inc. Class A Common Stock',
            'Microsoft Corporation Common Stock',
            'Amazon.com Inc. Common Stock',
            'Meta Platforms Inc. Class A Common Stock',
            'Broadcom Inc. Common Stock',
            'Tesla Inc. Common Stock',
            'Berkshire Hathaway Inc.',
            'Berkshire Hathaway Inc.',
            'Walmart Inc. Common Stock',
            'Eli Lilly and Company Common Stock'
        ] + [f'Company {i}' for i in range(97)],
        'Market Cap':  [
            4.505463e+12,  # NVDA - Highest
            4.083119e+12,  # AAPL
            3.898848e+12,  # GOOG
            3.895952e+12,  # GOOGL
            2.978717e+12,  # MSFT
            2.248366e+12,  # AMZN
            1.673200e+12,  # META
            1.578465e+12,  # AVGO
            1.542662e+12,  # TSLA
            1.121644e+12,  # BRK/A
            1.121005e+12,  # BRK/B
            1.045527e+12,  # WMT
            1.000386e+12   # LLY
        ] + [1000000 * (97 - i) for i in range(97)],
        'Last Sale': [100.0] * 110,
        'Net Change': [1.0] * 110,
        '% Change': [0.5] * 110,
        'IPO Year': [2000] * 110,
        'Volume': [1000000] * 110,
        'Sector': ['Technology'] * 110,
        'Industry': ['Software'] * 110,
        'Country': ['USA'] * 110}
        fake_data_df=pd.DataFrame(fake_data)

        #Mock pd.read_csv to return fake data
        mock_read_csv=mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.pd.read_csv', return_value=fake_data_df)

        #mocking the logger
        mock_logger =mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.logger')

        #ACT: calling the function
        result = _cleaned_nasdaq_list()

        #Assert:Verifying the results
         # 1. Check it returns a DataFrame
        assert isinstance(result, pd.DataFrame)
        
        # 2. Check it has exactly 110 rows
        assert len(result) == 110
        
        # 3. Check it only has Symbol, Name columns
        assert list(result.columns) == ['Symbol', 'Name', 'Market Cap']
        
        # 4. Check sorting worked (highest market cap first)
        assert result.iloc[0]['Symbol'] == 'NVDA'
        assert result.iloc[1]['Symbol'] == 'AAPL'
        assert result.iloc[2]['Symbol'] == 'GOOG'
        
        # 5. Check index starts at 1 (not 0)
        assert result.index[0] == 1
        assert result.index[-1] == 110
        
        # 6. Verify pd.read_csv was called with correct path
        print(f"Mock was called with: {mock_read_csv.call_args}")
        from etl_pipeline.src.extract._clean_nasdaq_data import RAW_DATA_PATH
        print(f"Expected path: {RAW_DATA_PATH}")
        mock_read_csv.assert_called_once_with(RAW_DATA_PATH)
        
        # 7. Verify logging happened
        assert mock_logger.info.call_count == 2  # Start and end logs
        mock_logger.info.assert_any_call(f"Started the cleaning process.INITIAL ROWS: 110")

class TestCleanNasdaqDataFileLoadingErrors:
    """
    Docstring for TestCleanNasdaqDataFileLoadingErrors
    """
    def test_returns_none_when_csv_is_none(self, mocker):
        #Arrange: Make pd.read_csv return none(simulating failure)
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.pd.read_csv', return_value= None)
        mock_logger =mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.logger')

        #ACT
        result = _cleaned_nasdaq_list()

        #Assert
        assert result is None

        #Verify logger info was NOT CALLED(function exists)
        mock_logger.info.assert_not_called()

class TestCleanNasdaqDataListColumnValidation:
    """
    Docstring for TestCleanNasdaqDataListColumnValidation
    """

    def test_returns_none_when_required_columns_missing(self,mocker):
        #Arrange: Creating data without required columns
        incomplete_data =pd.DataFrame({
            'Symbol':['APPL', 'MSFT'],
            'Name':['APPLE','Microsoft Corporation Common Stock']
            #DATA is missing marketcap
        })
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.pd.read_csv', return_value=incomplete_data)
        mock_logger = mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.logger')

        #ACT
        result = _cleaned_nasdaq_list()

        #Assert
        assert result is None 

        #verify error was logged with missing columns
        mock_logger.error.assert_called()

        #confirm error message mentions missing columns
        error_calls = mock_logger.error.call_args_list
        assert any('Missing required columns' in str(call) for call in error_calls)
    def test_returns_none_when_multiple_columns_missing(self, mocker):
        # ARRANGE: Only Symbol and Name columns
        minimal_data = pd.DataFrame({
            'Symbol': ['AAPL'],
            'Name': ['Apple']
        })
        
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.pd.read_csv', return_value=minimal_data)
        mock_logger = mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.logger')
        
        # ACT
        result = _cleaned_nasdaq_list()
        
        # ASSERT
        assert result is None
        
        # Verify it logged ALL missing columns (9 should be missing)
        error_calls = [str(call) for call in mock_logger.error.call_args_list]
        error_message = ''.join(error_calls)
        
        # Check that Market Cap, Volume, etc. are mentioned
        assert 'Market Cap' in error_message

class TestCleanedNasdaqListSortingErrors:
    """Test what happens when sorting fails"""
    
    def test_returns_none_when_sorting_raises_exception(self, mocker):
        # ARRANGE: Create data that will cause sorting to fail
        # Mock sort_values to raise an exception
        fake_df = pd.DataFrame({
            'Symbol': ['AAPL'],
            'Name': ['Apple'],
            'Market Cap': ['invalid_data'],  # String instead of number
            'Last Sale': [150.0],
            'Net Change': [1.5],
            '% Change': [1.0],
            'IPO Year': [2000],
            'Volume': [50000000],
            'Sector': ['Technology'],
            'Industry': ['Software'],
            'Country': ['USA']
        })
        
        # Make sort_values raise an exception
        fake_df.sort_values = Mock(side_effect=Exception("Cannot sort mixed types"))
        
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.pd.read_csv', return_value=fake_df)
        mock_logger = mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.logger')
        
        # ACT
        result = _cleaned_nasdaq_list()
        
        # ASSERT
        assert result is None
        
        # Verify error was logged
        mock_logger.error.assert_called()

class TestCleanedNasdaqListEdgeCases:
    """Test edge cases and boundary conditions"""
    
    def test_processes_successfully_with_fewer_than_110_rows(self, mocker):
        # ARRANGE: Only 50 stocks available
        small_data = {
            'Symbol': [f'STOCK{i}' for i in range(50)],
            'Name': [f'Company {i}' for i in range(50)],
            'Market Cap': [1000000 * i for i in range(50, 0, -1)],
            'Last Sale': [150.0] * 50,
            'Net Change': [1.5] * 50,
            '% Change': [1.0] * 50,
            'IPO Year': [2000] * 50,
            'Volume': [50000000] * 50,
            'Sector': ['Technology'] * 50,
            'Industry': ['Software'] * 50,
            'Country': ['USA'] * 50
        }
        small_df = pd.DataFrame(small_data)
        
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.pd.read_csv', return_value=small_df)
        mock_logger = mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.logger')
        
        # ACT
        result = _cleaned_nasdaq_list()
        
        # ASSERT
        # Should still work, but return only 50 rows
        assert result is not None
        assert len(result) == 50
        
        # Verify warning was logged
        warning_calls = [str(call) for call in mock_logger.error.call_args_list]
        assert any('WARNING: ONLY 50 rows available' in call for call in warning_calls)
    
    def test_handles_exactly_110_rows(self, mocker):
        # ARRANGE: Exactly 110 rows
        exact_data = {
            'Symbol': [f'STOCK{i}' for i in range(110)],
            'Name': [f'Company {i}' for i in range(110)],
            'Market Cap': [1000000 * i for i in range(110, 0, -1)],
            'Last Sale': [150.0] * 110,
            'Net Change': [1.5] * 110,
            '% Change': [1.0] * 110,
            'IPO Year': [2000] * 110,
            'Volume': [50000000] * 110,
            'Sector': ['Technology'] * 110,
            'Industry': ['Software'] * 110,
            'Country': ['USA'] * 110
        }
        exact_df = pd.DataFrame(exact_data)
        
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.pd.read_csv', return_value=exact_df)
        mock_logger = mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.logger')
        
        # ACT
        result = _cleaned_nasdaq_list()
        
        # ASSERT
        assert len(result) == 110
        
        # Should NOT log warning (we have exactly 110)
        error_calls = [str(call) for call in mock_logger.error.call_args_list]
        assert not any('WARNING' in call for call in error_calls)

class TestCleanedNasdaqListDataTransformations:
    """Test that data transformations work correctly"""
    
    def test_drops_unnecessary_columns(self, mocker):
        # ARRANGE
        full_data = {
            'Symbol': ['AAPL', 'MSFT', 'GOOGL'],
            'Name': ['Apple', 'Microsoft', 'Google'],
            'Market Cap': [3000000000000, 2500000000000, 1800000000000],
            'Last Sale': [150.0, 300.0, 2800.0],  # Should be dropped
            'Net Change': [1.5, 2.0, 15.0],  # Should be dropped
            '% Change': [1.0, 0.7, 0.5],  # Should be dropped
            'IPO Year': [1980, 1986, 2004],  # Should be dropped
            'Volume': [50000000, 30000000, 20000000],  # Should be dropped
            'Sector': ['Technology', 'Technology', 'Technology'],  # Should be dropped
            'Industry': ['Software', 'Software', 'Software'],  # Should be dropped
            'Country': ['USA', 'USA', 'USA']  # Should be dropped
        }
        full_df = pd.DataFrame(full_data)
        
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.pd.read_csv', return_value=full_df)
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.logger')
        
        # ACT
        result = _cleaned_nasdaq_list()
        
        # ASSERT
        # Only these 3 columns should remain
        assert list(result.columns) == ['Symbol', 'Name', 'Market Cap']
        
        # Verify dropped columns are NOT present
        assert 'Last Sale' not in result.columns
        assert 'Volume' not in result.columns
        assert 'Sector' not in result.columns
    
    def test_index_starts_at_1_not_0(self, mocker):
        # ARRANGE
        data = {
            'Symbol': ['AAPL', 'MSFT'],
            'Name': ['Apple', 'Microsoft'],
            'Market Cap': [3000000000000, 2500000000000],
            'Last Sale': [150.0, 300.0],
            'Net Change': [1.5, 2.0],
            '% Change': [1.0, 0.7],
            'IPO Year': [1980, 1986],
            'Volume': [50000000, 30000000],
            'Sector': ['Technology', 'Technology'],
            'Industry': ['Software', 'Software'],
            'Country': ['USA', 'USA']
        }
        df = pd.DataFrame(data)
        
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.pd.read_csv', return_value=df)
        mocker.patch('etl_pipeline.src.extract._clean_nasdaq_data.logger')
        
        # ACT
        result = _cleaned_nasdaq_list()
        
        # ASSERT
        assert result.index[0] == 1  # First index is 1, not 0
        assert result.index[1] == 2  # Second index is 2

