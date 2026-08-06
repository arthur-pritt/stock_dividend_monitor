from sqlalchemy.orm import Session

from database.models import DividendYieldGain
from database.repository import DividendYieldGainRepo

class DividendYieldService:
    """
    Business logic for managing the stock universer.
    """

    def __init__(self, session:Session):
        self.repository = DividendYieldGainRepo(session)
        self.session = session

    def save_stock(self, dividendyieldgain:DividendYieldGain)-> None:
        """
        Save a single stock ticker"""

        self.repository.save(dividendyieldgain)

    def save_stocks(self, dividendyieldgains: list[DividendYieldGain])-> int:
        """
        Save multiple stocks.
        returns:
               number of stocks saved.
        """

        count= self.repository.save_many(dividendyieldgains)
        self.session.commit()
        return count

    def get_stock(self, ticker:str)-> DividendYieldGain | None:
        """
        Retrieve one stock by ticker
        """
        
        return self.repository.get_by_ticker(ticker)

    def get_all_stocks(self)->list[DividendYieldGain]:
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

    def update_stock(self, dividendyieldgain:DividendYieldGain)-> None:
        """
        Update an existing stock
        """

        self.repository.update(dividendyieldgain)
         