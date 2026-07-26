from sqlalchemy.orm import Session 
from sqlalchemy import(
    select,
    func
)

from database.models import(
    Stock,
    DailyStockPrice,
    Historical90DaysData,
    Dividend,
    Earnings,
    DividendCompanies,
    DividendYieldGain,
    NonDividendCompanies,
    StockDailyWatchlist,
    StockDailyFlat,
    StockDailySegmented
)

class StockRepository:
    def __init__(self, session:Session):
        self.session = session 

    def save(self, stock:Stock)-> None:
        """
        Adding a single stock object to the current sesison.
        Note: This method does not commit the transaction
        """

        self.session.add(stock)

    def save_many(self, stocks:list[Stock])-> None:
        """
        Adding multiple stock objects to the current session
        Note: Method does not commit the transaction."""
        self.session.add_all(stocks)

    def get_by_ticker(self, ticker:str)->None:
        """
        """

        return self.session.get(
            Stock,
            ticker
        )
    def get_all(self)-> list[Stock]:
        """
        Retrieve all stock objects."""

        statement = select(Stock)
        return self.session.execute(
            statement
        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether a Stock exists."""

        return self.get_by_ticker(ticker) is not None 

    def count(self) -> int:
        """
        Return the total number of stocks.
        """

        statement = select(func.count()).select_from(Stock)

        return self.session.scalar(statement)

    def delete(self, stock:Stock)-> None:
        """
        Delete a Stock object."""

        self.session.delete(stock)


class DailyStockPrice:
    def __init__(self, session:Session):
        self.session = session 

    def save(self, dailystockprice:DailyStockPrice)-> None:
        """
        Adding a single stock price object to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add(dailystockprice)

    def save_many(self, dailystockprice:list[DailyStockPrice])-> None:
        """
        Adding multiple stock price objects to the current session.
        Note: This method does not commit the transaction."""

        self.session.add_all(dailystockprice)
    
    def get_by_ticker(self, ticker:str, recorded_date)->None:

        return self.session.get(
            DailyStockPrice,
            (
                ticker,
                recorded_date
            )
            
        )

    def get_all(self)-> list[DailyStockPrice]:
        """
        Retrieve  all stock price objects.
        """

        statement = select(DailyStockPrice)
        return self.session.execute(
            statement
        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether a stock price exists.
        """

        return self.get_by_ticker(ticker) is not None

    def count(self)-> int:
        """
        Return the total number of stock prices.
        """

        statement = select(func.count()).select_from(DailyStockPrice)
        return self.session.scalar(statement)

    def delete(self, dailystockprice:DailyStockPrice)->None:
        """
        Delete the stock price object
        """
        self.session.delete(dailystockprice)

class Historical90DaysData:
    def __init__(self, session:Session):
        self.session = session 

    def save(self, historical90daysdata:Historical90DaysData):

        """
        Adding a single historical data object to the current session.
        Note: This method does not commit the transaction.
        """
        self.session.add(historical90daysdata)


    def save_many(self, historical90daysdata:list[Historical90DaysData])-> None:
        """
        Adding multiple historical  data objects to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add_all(historical90daysdata)

    def get_by_ticker(self, ticker:str, recorded_date)-> None:
        return self.session.get(
            Historical90DaysData,
            (
                ticker,
                recorded_date
            )
            
        )

    def get_all(self)-> list[Historical90DaysData]:
        """
        Retrieve all the historical data.
        """

        statement = select(Historical90DaysData)
        return self.session.execute(
            statement 

        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether a stock historical data if it exists."""

        return self.get_by_ticker(ticker) is not None 

    def count(self)-> int:
        """
        return the total number of historical data prices.
        """

        statement = select(func.count()).select_from(Historical90DaysData)
        return self.session.scalar(statement)

    def delete(self, historical90daysdata:Historical90DaysData)->None:
        """
        Delete the historical 90 days data object.
        """

        self.session.delete(historical90daysdata)


class Dividend:
    def __init__(self, dividend:Dividend)-> None:
        self.session = self.session

    def save(self, dividend:Dividend)-> None:
        """
        Adding a single dividend object to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add(dividend)

    def save_many(self, dividend:list[Dividend])-> None:
        """
        Adding multple stock dividend objects to the current session.
        Note: Method does not commit the transaction
        ."""

        self.session.add_all(dividend)

    def get_by_ticker(self, ticker:str)-> None:

        return self.session.get(
            Dividend,
            ticker
        )

    def get_all(self)-> list[Dividend]:
        """
        Retrieve all dividend objects.
        """

        statement = select(Dividend)
        return self.session.execute(
            statement
        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether  a dividend stock exists.
        """

        return self.get_by_ticker(ticker) is not None

    def count(self)-> int: 
        """
        Return the total number of dividend paying stocks.
        """

        statement = select(func.count()).select_from(Dividend)

    def delete(self, dividend:Dividend)->None:
        """
        Delete a dividend stock object."""

        self.session.delete(dividend)

class Earnings:
    def __init__(self, session: Session):
        self.session = session 

    def save(self, earnings:Earnings)-> None:
        """
        Adding a single earnning object to the current session.
        Note: This method does not commit the transaction."""

        self.session.add(earnings)

    def save_many(self, earnings:list[Earnings])-> None:
        """
        Addning multiple stock earning price objects to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add_all(Earnings)

    def get_by_ticker(self, ticker:str)-> None:
        return self.session.get(
            Earnings,
            ticker
        )
    def get_all(self)-> list[Earnings]:
        """
        Retrieve all earning price objects.
        """

        statement = select(Earnings)
        return self.session.execute(
            statement
        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether a stock price exists.
        """
        return self.get_by_ticker(ticker) is not None 

    def count(self)-> int:
        """"
        Return the total number of stock prices.
        """

        statement =select(func.now()).select_from(Earnings)
        return self.session.scalar(statement)

    def delete(self, earnings:Earnings)-> None:
        """
        Delete the earning price object.
        """

        self.session.delete(earnings)

class StockDailyFlat():
    def __init__(self, session: Session):
        self.session = session 

    def save(self,stockdailyflat:StockDailyFlat):
        """
        Adding a single stock daily flat object to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add(stockdailyflat)

    def save_many(self, stockdailyflat:list[StockDailyFlat])-> None:
        """
        Adding multiple stock daily flat objects to the current session.
        Note: This method does not commit the transaction.
        """
        self.session.add_all(stockdailyflat)

    def  get_by_ticker(self, ticker:str, recorded_date)-> None:
        return self.session.get(
            StockDailyFlat,
            (
                ticker, 
                recorded_date,
                id
            )
        )

    def get_all(self)-> list[StockDailyFlat]:
        """"
        Retrieve all the stock daily flat data"""

        statement = select(StockDailyFlat)
        return self.session.execute(
            statement
        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether the complete stock data if it exists."""
        return self.get_by_ticker(ticker) is not None 

    def count(self)-> int:
        """
        Return the total number of stock daily price data.
        """

        statement = select(func.count()).select_from(StockDailyFlat)
        return self.session.scalar(statement)

    def delete(self, stockdailyflat:StockDailyFlat)->None:
        """
        Delete all the stock data table.
        """

        self.session.delete(stockdailyflat)

class StockDailySegmented:
    def __init__(self, session:Session)-> None:
        self.session = session 

    def save(self, stockdailysegmented:StockDailySegmented)-> None:
        """
        Adding a single segmented stock data object to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add(stockdailysegmented)

    def save_many(self, stockdailysementeds:list[StockDailySegmented])-> None:
        """
        Adding multiple segmented stock data objects to the current session.
        Note: This method does not commit the transaction."""

        self.session.add_all(stockdailysementeds)

    def get_by_ticker(self, ticker:str)->None:

        return self.session.get(
            StockDailySegmented,
            ticker
        )


    def get_all(self)-> list[StockDailySegmented]:
        """
        Retrieve all the segmented stock data.
        """

        statement = select(StockDailySegmented)
        return self.session.execute(
            statement
        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether a segmented stock exists.
        """

        return self.get_by_ticker(ticker) is not None 
    
    def count(self)-> int:
        """
        Return the total number of stocks."""

        statement= select(func.count()).select_from(StockDailySegmented)

        return self.session.scalar(statement)


    def delete(self, stockdailysegmented:StockDailySegmented)-> None:
        """
        Delete  a stock daily segmented object.
        """
        self.session.delete(stockdailysegmented)

class StockDailyWatchlist:
    def __init__(self, session:Session):
        self.session = session 

    def save(self, stockdailywatchlist: StockDailyWatchlist)-> None:
        """
        Adding a single stock watchlist object to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add(stockdailywatchlist)

    def save_many(self, stockdailywatchlist:list[StockDailyWatchlist])-> None:
        """
        Adding multiple stock watchlist object to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add_all(stockdailywatchlist)

    def get_by_ticker(self, ticker:str)-> None:
        return self.session.get(
            StockDailyWatchlist,
            ticker)

    def get_all(self)-> list[StockDailyWatchlist]:
        """
        Retrieve all the stock watchlist."""

        statement= select(StockDailyWatchlist)
        return self.statement.execute(
            statement
        ).scalar().all()
    def exists(self, ticker:str)-> bool:
        """
        Checks whether a stock watchlist exists.
        """

        return self.get_by_ticker(ticker) is not None

    def count(self)-> int:
        """
        Return the total number of stocks.
        """

        statement = select(func.count()).select_from(StockDailyWatchlist)
        return self.session.scalar(statement)

    def delete(self, stockdailywatchlist:StockDailyWatchlist)-> None:
        """
        Delete a stock object.
        """

        self.session.delete(StockDailyWatchlist)


class DividendYieldGain:
    def __init__(self, session:Session):
        self.session = session 

    def save(self, dividendyieldgain:DividendYieldGain)-> None:
        """
        Adding a single dividend yield object to the seesion.
        Note: This method does not commit the transaction."""

        self.session.add(dividendyieldgain)

    def save_many(self, dividendyieldgains:list[DividendYieldGain])-> None:
        """
        Adding multiple dividend yield object to the session.
        Note: This method does not commit the transaction.
        """

        self.session.add_all(dividendyieldgains)


    def get_by_ticker(self, ticker:str)->None:

        return self.session.get(
            DividendYieldGain,
            ticker
        )

    def get_all(self)-> list[DividendYieldGain]:
        """
        Retrieve all the dividend yield objects."""

        statement = select(DividendYieldGain)
        return self.session.execute(
            statement
        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether a stock exists.
        """

        return self.get_by_ticker(ticker) is not None 

    def count(self)-> int:
        """
        Return the total number of stocks.
        """

        statement = select(func.count()).select_from(DividendYieldGain)
        return self.session.scalar(statement)

    def delete(self, dividendyieldgain:DividendYieldGain)-> None:
        """
        Delete a dividend yield gain object."""
        self.session.delete(dividendyieldgain)

class DividendCompanies:
    def __int__(self, session:Session):
        self.session = session 

    def save(self, dividendcompany:DividendCompanies)-> None:
        """
        Adding a single dividend object to the current session
        Note: Method does not commit the transaction."""

        self.session.add(dividendcompany)

    def save_many(self, dividendcompanies:DividendCompanies)-> None:
        """
        Adding multiple dividend object to the current session
        Note: Method does not commit the transaction."""

        self.session.add_all(DividendCompanies)

    def get_by_ticker(self, ticker:str)-> None:
        return self.session.get(
            DividendCompanies,
            ticker 
        )

    def get_all(self)-> list[DividendCompanies]:
        """"
        Retrieve all the dividend objects.
        """

        statement = select(DividendCompanies)
        return self.session.execute(
            statement
        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether a dividend stock companies exists.
        """
        return self.get_by_ticker(ticker) is not None 
    
    def count(self)-> int:
        """
        Return the total number dividend companies.
        """

        statement = select(func.count()).select_from(DividendCompanies)
        return self.session.scalar(statement)

    def delete(self, dividendcompany:DividendCompanies)-> None:
        """
        Delete a dividend companies.
        """

        self.session.delete(dividendcompany)

class NonDividendCompanies:
    def __int__(self, session:Session):
        self.session = session 

    def save(self, nondividendcompany:NonDividendCompanies)-> None:
        """
        Adding a single non dividend company to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add(nondividendcompany)

    def save_many(self, nondividendcompanies:NonDividendCompanies)-> None:
        """
        Adding multiple non dividend companies to the current session.
        Note: This method does not commit the transaction.
        """

        self.session.add_all(nondividendcompanies)

    def get_by_ticker(self, ticker:str)-> None:
        return self.session.get(
            NonDividendCompanies,
            ticker
        )

    def get_all(self)-> list[NonDividendCompanies]:
        """
        Retrieve all non dividend companies objects.
        """

        statement = select(NonDividendCompanies)
        return self.session.execute(
            statement
        ).scalar().all()

    def exists(self, ticker:str)-> bool:
        """
        Checks whether a non dividend company exists.
        """

        return self.get_by_ticker(ticker) is not None 

    def count(self)-> int:
        """
        Return the total number of non dividend companies.
        """

        statement = select(func.count()).select_from(NonDividendCompanies)
        return self.session.scalar(statement)

    def delete(self, nondividendcompany:NonDividendCompanies)-> None:
        """
        Delete a nondividend companies object.
        """

        self.session.delete(nondividendcompany)



    
        





    


        

    

