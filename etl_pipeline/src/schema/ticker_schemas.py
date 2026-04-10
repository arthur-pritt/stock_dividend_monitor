
import pandera.pandas as pa
from pandera import Column, Check, DataFrameSchema
from config.settings import (DATA_COLS)

TICKER_SCHEMA = pa.DataFrameSchema({
    DATA_COLS['ticker']:pa.Column(str, nullable=False),
    DATA_COLS['name']:pa.Column(str, nullable=False),
    DATA_COLS['valuations']:pa.Column(float, nullable=True)
    
})