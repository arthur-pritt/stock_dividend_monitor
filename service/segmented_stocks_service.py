from sqlalchemy.orm import Session 
from database.models import StockDailySegmented
from database.repository import StockDailySegmentedRepo

class SegmentedStocksService:
    """
    Business logic for managing segmented stock table.
    """

    def __init__(self, session:Session):
        self.repository = StockDailySegmentedRepo(session)
        self.session = session 

    def save_stock(self, stockdailysegmented:StockDailySegmented)-> None:
        """
        Save a single segmented stock.
        """

        self.repository.save(stockdailysegmented)

    def save_stocks(self, stockdailysegmenteds: list[StockDailySegmented])-> int:
        """
        Save multiple stock segmented tickers.
        """

        count= self.repository.save_many(stockdailysegmenteds)
        self.session.commit()
        return count   

    def get_stock(self, ticker:str)-> StockDailySegmented | None:
        """
        Retrieve one stock by ticker.
        """

        return self.repository.get_by_ticker(ticker) 

    def get_all_stocks(self)-> list[StockDailySegmented]:
        """
        Retrieve every segmented stock"""
        return self.repository.get_all()

    def stock_exists(self, ticker:str)-> bool:
        """
        Check whether a stock exists.
        """

        return self.repository.exists(ticker)

    def total_stocks(self)-> int:
        """
        Count all the stocks.
        """

        return self.repository.count()

    def delete_stock(self, ticker:str)-> bool:
        """
        Delete one stock and return true if deleted and false if not found."""

        return self.repository.delete(ticker)

    def update_stock(self, segmented_stock:StockDailySegmented)-> None:
        """"
        Update an existing stock.
        """

        self.repository.update(segmented_stock)
        

        