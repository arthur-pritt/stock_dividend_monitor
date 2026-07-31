from sqlalchemy.orm import Session 
from database.models import Earnings
from database.repository import EarningsRepo

class EarningService:
    """
    Business logic for managing the earning per share table."""

    def __init__(self, session:Session):
        self.repository = EarningsRepo(session)
        self.session = session 

    def save_stock(self, earning:Earnings)-> None:
        """
        Save a single earning per share stock ticker."""

        self.repository.save(earning)

    def save_stocks(self, earnings:list[Earnings])-> int:
        """
        Save multiple earning per share stocks.
        """
        count= self.repository.save_many(earnings)
        self.session.commit()
        return count 

    def get_stock(self, ticker:str)-> Earnings | None:
        """
        Retrieve one earning per share ticker.
        """

        return self.repository.get_by_ticker(ticker)

    def get_all_stocks(self)-> list[Earnings]:
        """
        Retrieve every earnings per share.
        """

        return self.repository.get_all()

    def stock_exists(self, ticker:str)-> bool:
        """
        Check whether earning per share stock exists.
        """

        return self.repository.exists(ticker)

    def total_stocks(self)-> int:
        """
        Count all the stocks.
        """

        return self.repository.count()

    def delete_stock(self, ticker:str)-> bool:
        """
        Delete earning per share stock and returns:
             True if deleted
             False if not found.
        """

        return self.repository.delete(ticker)

    def update_stock(self, earning:Earnings)-> None:
        """
        Update an existing stock"""

        self.repository.update(earning)
    

        