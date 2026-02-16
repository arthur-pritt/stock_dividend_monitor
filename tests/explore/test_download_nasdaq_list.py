import pandas as pd
import sys
from pathlib import Path
from unittest.mock import patch 

#Adding project root and src to path
project_root=Path(__file__).parent.parent.parent.resolve()
sys.path.insert(0, str(project_root))


from etl_pipeline.src.extract._download_nasdaq_list import load_nasdaq_data

class TestExtractData:
    def test_load_nasdaq_data_success():
        mock_df = pd.DataFrame(
            {'Symbol' :['APPL', 'MSFT'], 'Name': ['Apple inc. Common Stock','Microsoft Corporation Common Stock']})
        with patch("etl_pipeline.src.extract._download_nasdaq_list.pd.read_csv", return_value = mock_df):
            result = load_nasdaq_data()
            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert list(result.columns)== ["Symbol", "Name"]

    def test_load_nasdaq_data_file_not_found():
        with patch("etl_pipeline.src.extract._download_nasdaq_list.pd.read_csv", side_effect=FileNotFoundError):
            result = load_nasdaq_data()

            assert result is None 
    
    def test_load_nasdaq_data_empty_csv():
        with patch("etl_pipeline.src.extract._download_nasdaq_list.pd.read_csv", side_effect=pd.errors.EmptyDataError("No column to parse")):
            result = load_nasdaq_data()
            assert result is None 

    def test_load_nasdaq_data_unexpected_error():
        with patch("etl_pipeline.src.extract._download_nasdaq_list.pd.read_csv", side_effect=RuntimeError):
            result = load_nasdaq_data()
            assert result is None 

