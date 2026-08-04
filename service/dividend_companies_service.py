from sqlalchemy.orm import Session

from database.models import DividendCompanies
from database.repository import DividendCompaniesRepo

class DividendCompaniesService:
    """
    Business logic for managing the stock universer.
    """

    def __init__(self, session:Session):
        self.repository = DividendCompaniesRepo(session)
        self.session = session

    def save_stock(self, dividendcompany:DividendCompanies)-> None:
        """
        Save a single stock ticker"""

        self.repository.save(dividendcompany)
        

    def save_stocks(self, records: list[DividendCompanies])-> int:
        """
        Save multiple stocks.
        returns:
               number of stocks saved.
        """

        count= self.repository.save_many(records)
        self.session.commit()
        return count

    def get_stock(self, ticker:str)-> DividendCompanies | None:
        """
        Retrieve one stock by ticker
        """
        
        return self.repository.get_by_ticker(ticker)

    def get_all_stocks(self)->list[DividendCompanies]:
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

    def update_stock(self, dividendcompany:DividendCompanies)-> None:
        """
        Update an existing stock
        """

        self.repository.update(dividendcompany)
        