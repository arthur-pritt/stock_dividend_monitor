from sqlalchemy.orm import Session 
from database.models import StockDailyFlat
from database.repository import StockDailyFlatRepo

class StockDailyFService:
    """
    Business logic for managing the  daily stock table.
    """

    def __init__(self, session:Session):
        self.repository = StockDailyFlatRepo(session)
        self.session = session 

    def save_stock(self, stockdailyflat:StockDailyFlat)-> None:
        """
        Save a single daily stock.
        """

        self.repository.save(stockdailyflat)

    def save_stocks(self, stockdailyflats:list[StockDailyFlat])-> int:
        """
        Save multiple stock daily price.
        """

        count= self.repository.save_many(stockdailyflats)
        self.session.commit()
        return count 

    def get_stock(self, ticker:str)-> StockDailyFlat | None:
        """
        Retrieve one stock by ticker.
        """

        return self.repository.get_by_ticker(ticker)

    def get_all_stocks(self)-> list[StockDailyFlat]:
        """
        Retriebe every stock
        """

        return self.repository.get_all()

    def stock_exists(self, ticker:str)-> bool:
        """
        Checj whether a stock exists.
        """

        return self.repository.exists(ticker)

    def total_stocks(self)-> int:
        """
        Count all the stocks.
        """

        return self.repository.count() 

    def delete_stock(self, ticker:str)-> bool:
        """
        Delete one stock and return true if deleted and false if not found"""

        return self.repository.delete(ticker)

    def update_stock(self, stockdailyflat:StockDailyFlat)-> None:
        """
        Update an existing stock
        """

        self.repository.update(stockdailyflat)
    



    
        