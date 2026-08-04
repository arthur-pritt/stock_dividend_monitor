from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    Integer,
    DateTime,
    Numeric,
    String,
    Text,
    func
)

from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column
)

class Base(DeclarativeBase):
    """Base class for all SQLAlchemny models."""
    pass 

class Stock(Base):
    __tablename__ = "stocks"

    ticker: Mapped[str] = mapped_column(
        Text,
        primary_key=True 
    )

    name: Mapped[str] = mapped_column(
        Text,
        nullable=False
    )

    market_cap: Mapped[Decimal] = mapped_column(
        Numeric(20,2),
        nullable= False 
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )



class DailyStockPrice(Base):
    __tablename__="daily_stock_price"

    ticker:Mapped[str] = mapped_column(
        Text,
        primary_key=True
    )

    recorded_date: Mapped[date]= mapped_column(
        Date,
        primary_key=True
    )

    adj_close : Mapped[Decimal] = mapped_column(
        Numeric(20,2),
        nullable=False
    )

    created_at : Mapped[datetime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )

class Historical90DaysData(Base):
    __tablename__="historical_90days_data"

    ticker:Mapped[str]=mapped_column(
        Text,
        primary_key=True
    )

    recorded_date:Mapped[date]= mapped_column(
        Date,
        primary_key=True
    )

    adjclose:Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False
    )

    open:Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    high:Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False
    )

    low:Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False  
    )

    close: Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    volume: Mapped[BigInteger]= mapped_column(
        BigInteger,
        nullable=False 
    )

    actual_days: Mapped[int]= mapped_column(
        Integer,
        nullable=False 
    )

    coverage_pct:Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False
    )

    is_flagged:Mapped[bool]= mapped_column(
        Boolean,
        default=False
    )
    created_at : Mapped[datetime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )

class Dividend(Base):
    __tablename__= "dividend"

    ticker:Mapped[str]=mapped_column(
        Text,
        primary_key=True)

    cik:Mapped[int]=mapped_column(
        BigInteger,
        nullable=False
    )

    dividend_per_share: Mapped[Decimal]=mapped_column(
        Numeric(10,2),
        nullable=False
    )
    raw_payout: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    quarter:Mapped[int]= mapped_column(
        BigInteger,
        primary_key=True 
    )

    year:Mapped[int]=mapped_column(
        BigInteger,
        primary_key=True
    )

    created_at:Mapped[DateTime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )

class Earnings(Base):
    __tablename__="earnings"

    ticker:Mapped[str]=mapped_column(
            Text,
            primary_key=True)
    
    cik:Mapped[int]=mapped_column(
        BigInteger,
        nullable=False
        )
    
    earnings_pershare: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False)
    
    quarter:Mapped[int]= mapped_column(
        BigInteger,
        primary_key=True )
    
    year:Mapped[int]=mapped_column(
        BigInteger,
        primary_key=True)
    
    created_at:Mapped[DateTime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now())

    

class StockDailyFlat(Base):
    __tablename__="stock_daily_flat"

    id:Mapped[int]= mapped_column(
        autoincrement=True,
        primary_key=True
    )

    ticker:Mapped[str]= mapped_column(
        Text,
        nullable=False
    )

    name:Mapped[str] = mapped_column(
        Text,
        nullable=False
    )

    recorded_date:Mapped[date] = mapped_column(
        Date,
        nullable=False
    )

    market_cap: Mapped[Decimal] = mapped_column(
        Numeric(20,2),
        nullable=False
    )

    adj_close: Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    dividend_per_share: Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    earnings_pershare: Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    raw_payout: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable= True
    )

    frequency : Mapped[str] = mapped_column(
        Text,
        nullable=True
    )

    quarter : Mapped[BigInteger] = mapped_column(
        BigInteger,
        nullable=True
    )

    year : Mapped[int] = mapped_column(
        BigInteger,
        nullable=True
    )

    created_at : Mapped[DateTime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )

class StockDailySegmented(Base):
    __tablename__="stock_daily_segmented"

    id:Mapped[int]= mapped_column(
        autoincrement=True,
        primary_key= True
    )

    ticker: Mapped[str]= mapped_column(
        Text,
        primary_key=True
    )

    name: Mapped[str]=mapped_column(
        Text,
        nullable=False 
    )

    recorded_date: Mapped[date]= mapped_column(
        Date,
        primary_key=True
    )

    adj_close: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    dividend_per_share: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    raw_payout : Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    earnings_pershare : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    dividend_status : Mapped[str] = mapped_column(
        Text,
        nullable=False 
    )

    frequency : Mapped[str]= mapped_column(
        Text,
        nullable= False 
    )

    quarter : Mapped[int]= mapped_column(
        BigInteger,
        nullable=False 
    )

    year : Mapped[int]= mapped_column(
        BigInteger,
        nullable=False 
    )

    created_at: Mapped[datetime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )

class StockDailyWatchlist(Base):
    __tablename__="stock_daily_watchlist"

    ticker: Mapped[str]= mapped_column(
        Text,
        primary_key=True
    )

    name: Mapped[str]= mapped_column(
        Text,
        nullable=False 
    )

    market_cap: Mapped[Decimal]= mapped_column(
        Numeric(20,2),
        nullable=False 
    )

    latest_date: Mapped[date]= mapped_column(
        Date,
        primary_key=True
    )

    current_adjclose : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    baseline_date: Mapped[date] = mapped_column(
        Date,
        nullable=False 
    )

    historical_adjclose: Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    price_diff : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    pct_change : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    watchlist_status : Mapped[str] = mapped_column(
        Text,
        nullable=False 
    )

    dividend_per_share : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    earnings_pershare : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    raw_payout : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    actual_days : Mapped[int] = mapped_column(
        Integer,
        nullable=False 
    )

    frequency : Mapped[str] = mapped_column(
        Text,
        nullable=False 
    )

    quarter : Mapped[int]= mapped_column(
        BigInteger,
        nullable=False 
    )

    year : Mapped[int]= mapped_column(
        BigInteger,
        nullable=False 
    )

    created_at: Mapped[datetime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )

class DividendYieldGain(Base):
    __tablename__="dividend_yield_gain"

    ticker: Mapped[str]= mapped_column(
        Text,
        primary_key=True
        )
    
    name: Mapped[str]= mapped_column(
        Text,
        nullable=False
        )
    
    market_cap: Mapped[Decimal]= mapped_column(
        Numeric(20,2),
        nullable=False)
    
    latest_date: Mapped[date]= mapped_column(
        Date,
        primary_key=True
        )
    
    current_adjclose : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False
        )
    
    baseline_date: Mapped[Date] = mapped_column(
        Date,
        nullable=False
        )
    
    historical_adjclose: Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False)
    
    dividend_per_share : Mapped[str] = mapped_column(
        Numeric(10,2),
        nullable=False)

    dividend_status : Mapped[str] = mapped_column(
        Text,
        nullable=False)

    raw_payout : Mapped[str] = mapped_column(
        Numeric(10,2),
        nullable=False)

    dividend_yield_pct : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False)

    actual_days : Mapped[int] = mapped_column(
        Integer,
        nullable=False 
    )

    price_diff : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False 
    )
    
    pct_change : Mapped[Decimal] = mapped_column(
        Numeric(10,2),
        nullable=False)
    
    watchlist_status : Mapped[str] = mapped_column(
        Text,
        nullable=False)

    three_year_gain: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    five_year_gain: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    ten_year_gain: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    three_year_pct: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    five_year_pct: Mapped[Decimal]= mapped_column(
        Numeric(10, 2),
        nullable=False 
    )

    ten_year_pct: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    action_signal : Mapped[str]= mapped_column(
        Text,
        nullable=False 
    )

    earnings_pershare : Mapped[str] = mapped_column(
        Numeric(10,2),
        nullable=False)
    
    created_at: Mapped[datetime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now())

class DividendCompanies(Base):
    __tablename__="dividend_companies"

    ticker : Mapped[str]= mapped_column(
        Text,
        primary_key=True
    )

    name: Mapped[str]= mapped_column(
        Text,
        nullable=False)
        
    market_cap: Mapped[Decimal]= mapped_column(
        Numeric(20,2),
        nullable=False)
    recorded_date: Mapped[date]= mapped_column(
        Date,
        primary_key=True
    )

    adj_close: Mapped[Decimal]= mapped_column(
        Numeric(10, 2),
        nullable=False 
    )

    dividend_per_share: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    dividend_status: Mapped[str]= mapped_column(
        Text,
        nullable=False 
    )

    earnings_pershare: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    raw_payout: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False 
    )

    frequency: Mapped[str]= mapped_column(
        Text, 
        nullable=False 
    )

    quarter: Mapped[int]= mapped_column(
        BigInteger,
        nullable=False 
    )

    year:Mapped[int]= mapped_column(
        BigInteger,
        nullable=False 
    )

    created_at: Mapped[datetime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now() 
    )



class NonDividendCompanies(Base):
    __tablename__="non_dividend_companies"

    ticker : Mapped[str]= mapped_column(
        Text,
        primary_key=True)
    
    name: Mapped[str]= mapped_column(
        Text,
        nullable=False)
            
    market_cap: Mapped[Decimal]= mapped_column(
        Numeric(20,2),
        nullable=False)

    
    recorded_date: Mapped[date]= mapped_column(
        Date,
        primary_key=True)
    
    adj_close: Mapped[Decimal]= mapped_column(
        Numeric(10, 2),
        nullable=False )
    
    earnings_pershare: Mapped[Decimal]= mapped_column(
        Numeric(10,2),
        nullable=False)

    dividend_status: Mapped[str]= mapped_column(
        Text,
        nullable=False)
    
    quarter: Mapped[int]= mapped_column(
        BigInteger,
        nullable=False)

    year: Mapped[int]= mapped_column(
        BigInteger,
        nullable=False)

    created_at: Mapped[datetime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    