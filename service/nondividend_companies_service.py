from sqlalchemy.orm import Session

from database.models import NonDividendCompanies
from database.repository import NonDividendCompaniesRepo

class NonDividendCompaniesService:
    """
    Business logic for managing the stock universer.
    """

    def __init__(self, session:Session):
        self.repository = NonDividendCompaniesRepo(session)
        self.session = session

    def save_stock(self, nondividendcompany:NonDividendCompanies)-> None:
        """
        Save a single stock ticker"""

        self.repository.save(nondividendcompany)
        self.session.commit()

    def save_stocks(self, nondividendcompanies: list[NonDividendCompanies])-> int:
        """
        Save multiple stocks.
        returns:
               number of stocks saved.
        """

        self.repository.save_many(nondividendcompanies)
        self.session.commit()

    def get_stock(self, ticker:str)-> NonDividendCompanies | None:
        """
        Retrieve one stock by ticker
        """
        
        return self.repository.get_by_ticker(ticker)

    def get_all_stocks(self)->list[NonDividendCompanies]:
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

        deleted = self.repository.delete(ticker)

        if deleted:
            self.session.commit()
        return deleted

    def update_stock(self, nondividendcompanies:NonDividendCompanies)-> None:
        """
        Update an existing stock
        """

        self.repository.update(nondividendcompanies)
        self.session.commit() 