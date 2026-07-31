from sqlalchemy.orm import Session

from database.models import Stock
from database.repository import StockRepository

class StockService:
    """
    Business logic for managing the stock universer.
    """

    def __init__(self, session:Session):
        self.repository = StockRepository(session)
        self.session = session

    def save_stock(self, stocks: list[Stock])-> None:
        """
        Save a single stock object (staged in session)
        Delegates database execution to repository and deferes commit to orchestrator.
        """

        return self.repository.save(stocks)

    def save_stocks(self, records:list[dict] )-> int:
        """
        Save multiple stocks in batches using  list of dicts
        returns:
               number of stocks saved.
        """

        count= self.repository.save_many(records)
        self.session.commit()
        return count

    def get_stock(self, ticker:str)-> Stock | None:
        """
        Retrieve one stock by ticker
        """
        
        return self.repository.get_by_ticker(ticker)

    def get_all_stocks(self)->list[Stock]:
        """
        Retrieve every stock
        """

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
        Delete one stock.
        Returns:
              True if deleted
              False if not found
        """

        return self.repository.delete(ticker)

    def update_stock(self, stock:Stock)-> None:
        """
        Update an existing stock
        """

        self.repository.update(stock)
        

    


    