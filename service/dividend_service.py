from sqlalchemy.orm import Session 
from database.models import Dividend
from database.repository import DividendRepo

class DividendService:
    """"
    Business logic for managing the stock dividend.
    """

    def __init__(self, session:Session):
        self.repository = DividendRepo(session)
        self.session = session 

    def save_stock(self,dividend:Dividend)-> None:
        """
        Save a single dividend ticker
        """

        return self.repository.save(dividend)

    def save_stocks(self, dividend:list[Dividend])-> int:
        """
        Save multiple dividend tickers and return saved stocks ticker.
        """

        count= self.repository.save_many(dividend)
        self.session.commit()
        return count 

    def get_stock(self, ticker:str)-> Dividend | None:
        """
        Retrieve one dividend per share by ticker.
        """

        return self.repository.get_by_ticker(ticker)

    def get_all_stocks(self)-> list[Dividend]:
        """
        Retrieve every daily stock price data
        """
        return self.repository.get_all()

    def stock_exists(self, ticker:str)-> bool:
        """
        Checks whether a dividend stock exists.
        """

        return self.repository.exists(ticker)

    def total_stocks(self)-> int:
        """
        Count all the dividend stocks.
        """

        return self.repository.count()

    def delete_stock(self, ticker:str)-> bool:
        """
        Delete one stock. Returns true if deleted and false if not found."""

        return self.repository.delete(ticker)

    def update_stock(self, dividend:Dividend)-> None:
        """
        Update an existing stock.
        """

        self.repository.update(dividend)
    

        