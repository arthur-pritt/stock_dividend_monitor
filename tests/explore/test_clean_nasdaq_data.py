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
