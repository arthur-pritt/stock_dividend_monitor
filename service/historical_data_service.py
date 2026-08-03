from sqlalchemy.orm import Session 
from database.models import Historical90DaysData
from database.repository import Historical90DaysDataRepo

class HistoricalDataService:
    """
    Business logic for managing the historical data services.
    """

    def __init__(self, session:Session):
        self.repository = Historical90DaysDataRepo(session)
        self.session = session 

    def save_stock(self, historical90daysdata:Historical90DaysData)-> None:
        """
        Save a single ticker historical data.
        """

        self.repoitory.save(historical90daysdata)

    def save_stocks(self, historical90daysdatas: list[Historical90DaysDataRepo])-> int:
        """
        Retrieve multiple historical ticker data."""
        count=self.repository.save_many(historical90daysdatas)
        self.session.commit()
        return count 

    def get_stock(self, ticker:str)-> Historical90DaysData | None:
        """
        Retrieve one historical data by ticker.
        """

        return self.repository.get_by_ticker(ticker)

    def get_all_stocks(self)->list[Historical90DaysData]:
        """
        Retrieve evert stock.
        """
        return self.repository.get_all()

    def stock_exists(self, ticker:str)-> bool:
        """
        Check whether a historical data exists.
        """

        return self.repository.exists(ticker)

    def total_checks(self)-> int:
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

    def update_stock(self, historical90daydata:Historical90DaysData)-> None:
        """
        Update an existing stock
        """

        self.repository.update(historical90daydata)


        



    
    

