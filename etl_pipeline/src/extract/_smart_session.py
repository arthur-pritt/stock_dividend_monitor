import requests_cache
from requests_ratelimiter import LimiterSession
import yfinance as yf
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent
import random
import time     


class SmartSession(LimiterSession):
    """Custom session combining rate limiting + automatic user agent rotation  + retry + catching."""

    def __init__(
            self,
            per_second=2,
            per_minute=60,
            burst=8,
            total_retries:int=5,
            backoff_factor: float=1.5,
            cache_name: str ="yahoo_cache",
            expire_after: int=300,
            **kwargs
    ):
        super().__init__(
              per_second=per_second,
              per_minute=per_minute,
              burst=burst,
              per_host=True
        )
        #Local catching
        self.cache_name= cache_name
        requests_cache.install_cache(
            cache_name=cache_name,
            backend='sqlite',
            expire_after=expire_after,
            allowable_methods=['GET']
        )
        #Save retry settings
        self.total_retries= total_retries
        self.backoff_factor= backoff_factor

        #Creating useragent object once.
        self.ua=UserAgent()

        #Default safe headers
        self.headers.update({
            'Accept':'text/html,application/xhtml+xml,application/json,*/*',
            'Accept-Language':'en-US,en;q=0.9',
            'Referer':'https://finance.yahoo.com/',
        })

        #Setting up retry strategy
        self._setup_retry(total_retries,backoff_factor)

    def _setup_retry(
              self,
              total_retries,
              backoff_factor):
         """Configure/implement retry strategy"""
         retry_strategy=Retry(
             total=self.total_retries,
             backoff_factor=self.backoff_factor,
             status_forcelist=[429,500,502,504],
             allowed_methods=["GET","POST","HEAD","OPTIONS"],
             raise_on_status=False)
         #Creating an adapter(transport layer) with retry
         retry_adapter= HTTPAdapter(max_retries=retry_strategy)
         #Mounting the adapater to the active ratelimiter
         self.mount("http://", retry_adapter)
         self.mount("http://", retry_adapter)

    def request(self, method, url, **kwargs):
        """Runs before every request(get, post, etc)"""
        try:
                #Rotates user agent automatically
                self.headers['User-Agent']=self.ua.random

                #time delay
                time.sleep(random.uniform(0.4,1.2))

                response=super().request(method, url, **kwargs)
                
                #Raise error for bad status
                if response.raise_for_status==429:
                    print(f"Rate limit hit on {url}. Waiting longer...")
                    time.sleep(10)
                    return response 
        except requests.exceptions.HTTPError as e:
                print(f"HTTP Error {e.response.status_code} for {url}")
                raise
        except requests.exceptions.Timeout:
                print(f"TIMEOUT ON {url}")
                raise 
        except Exception as e:
                print(f"Unexpected error on {url} : {e}")
                raise
                
        return super().request(method, url, **kwargs)

#Creating smart session object
session=SmartSession(
    per_second=1,
    per_minute=80,
    burst=8,
    total_retries=5,
    backoff_factor=1.5,
    expire_after=3600,
    cache_name="yahoo_cache"
)
        


        
