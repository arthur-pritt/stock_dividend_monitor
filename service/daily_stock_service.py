from sqlalchemy.orm import Session 
from database.models import DailyStockPrice
from database.repository import DailyStockPriceRepo

class DailyStockService:
    """
    Business logic for managing the daily stock price.
    """

    def __init__(self,session:Session):
        self.repository = DailyStockPriceRepo(session)
        self.session = session

    def save_price(self,dailystockprice:DailyStockPrice)-> None:
        """
        Save a single daily stock data.
        Delegates database execution to repository and deferes commit to orchestrator.
        """

        return self.repository.save(dailystockprice)
        

    def save_prices(self, dailystockprices:list[DailyStockPrice])->int:
        """
        Save multiple stocks in batches using list of dicts
        returns a number of stocks saved
        """

        count= self.repository.save_many(dailystockprices)
        self.session.commit()
        return count

    def get_stock(self,ticker:str)-> DailyStockPrice | None:
        """
        Retrieve one daily stock price by ticker.
        """

        return self.repository.get_by_ticker(ticker)

    def get_all_stocks(self)-> list[DailyStockPrice]:
        """
        Retrieve every daily stock price data.
        """

        return self.repository.get_all()

    def stock_exists(self, ticker:str)-> bool:
        """"
        Checks whether a stock price data exists.
        """

        return self.repository.exists(ticker)

    def total_stocks(self)-> int:
        """
        Count all the stocks.
        """

        return self.repository.count()

    def delete_stock(self, ticker:str)-> bool:
        """
        Delete one stock daily stock data
        Returns: True if deleted
        False if not found
        """

        return self.repository.delete(ticker)

    def update_stock(self, dailystockprice:DailyStockPrice)-> None:
        """
        Update an existing stock
        """

        self.repository.update(dailystockprice)
        

        


    