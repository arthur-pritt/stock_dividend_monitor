from sqlalchemy.orm import Session
from database.models import StockDailyWatchlist
from database.repository import StockDailyWatchlistRepo

class WatchlistService:
    """
    Business logic for managing stock watchlist table.
    """

    def __init__(self, session:Session):
        self.repository = StockDailyWatchlistRepo(session)
        self.session = session 

    def save_stock(self, stockdailywatchlist:StockDailyWatchlist)-> None:
        """
        Save a single stock watchlist ticker.
        """

        self.repository.save(stockdailywatchlist)

    def save_stocks(self, watchlist:list[StockDailyWatchlist])-> int:
        """
        Save multiple watchlist stocks.
        """

        count= self.repository.save_many(watchlist)
        self.session.commit()
        return count

    def get_stock(self, ticker:str)-> StockDailyWatchlist | None:
        """
        Retrieve one watchlist stock by ticker.
        """
        return self.repository.get_by_ticker(ticker)

    def stock_exists(self, ticker:str)-> bool:
        """
        Check whether  a stockwatchlist exists.
        """

        return self.repository.exists(ticker)

    def total_stocks(self)-> int:
        """
        Count all the stocks.
        """

        return self.repository.count()

    def delete_stock(self, ticker:str)-> bool:
        """
        Delete one stock watchlist.
        """

        return self.repository.delete(ticker)

    def update_stock(self, stockdailywatchlist:StockDailyWatchlist)-> None:
        """
        Update an existing stock watchlist.
        """

        self.repository.update(stockdailywatchlist)


        