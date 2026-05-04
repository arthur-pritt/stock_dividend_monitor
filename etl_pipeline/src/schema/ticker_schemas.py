
import pandera.pandas as pa
from pandera import Column, Check, DataFrameSchema
from config.settings import (DATA_COLS)

TICKER_SCHEMA = pa.DataFrameSchema({
    DATA_COLS['ticker']:pa.Column(str, nullable=False),
    DATA_COLS['name']:pa.Column(str, nullable=False),
    DATA_COLS['valuations']:pa.Column(float, nullable=True)
    
})

CURRENT_PRICE_FILE_SCHEMA = pa.DataFrameSchema({
    DATA_COLS['ticker']:pa.Column(
        dtype=str,
        nullable=False,
        unique=True, #No duplicates
        checks=[
            Check.str_length(min_value=1), 
            Check(lambda s:~s.str.strip().eq("")) 
        ]),

    DATA_COLS['name']:pa.Column(
        dtype=str,
        nullable=False,
        unique=False, 
        checks=[
            Check.str_length(min_value=1), #No empty string
            Check(lambda s:~s.str.strip().eq("")) #No whitespace-only string
        ]),

    DATA_COLS['valuations']:pa.Column(
        dtype=float, 
        nullable=True,
        )
    },
    strict=False,
    coerce=True) 

HISTORICAL_SCHEMA = pa.DataFrameSchema({
    "symbol": Column(str),
    "date": Column(pa.DateTime, coerce=True),
    "adjclose": Column(float),
    "volume": Column(float, coerce=True),
    "coverage_pct": Column(float),
    "is_flagged": Column(bool),
    "actual_days": Column(int),
})
