
import requests
import random
import time 
import logging 
from tenacity import retry, stop_after_attempt,wait_exponential,retry_if_exception_type
from curl_cffi import requests  


class RobustCurlSession:
    """Production-ready curl_cffi session for Yahoo Finance"""
    
    def __init__(
        self,
        impersonate: str = "chrome131",
        delay_min: float = 0.8,
        delay_max: float = 2.5,
        max_retries: int = 5
    ):
        self.session = requests.Session()
        self.session.impersonate = impersonate
        
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.max_retries = max_retries
        
        self.logger = logging.getLogger(__name__)
        
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/json,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://finance.yahoo.com/",
        })

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=12),
        retry=retry_if_exception_type((
            requests.exceptions.RequestException,
            TimeoutError,
            OSError,
            ConnectionError
        ))
    )
    def get(self, url, params=None, **kwargs):
        self._apply_delay()
        
        response = self.session.get(url, params=params, **kwargs)
        
        if response.status_code == 429:
            self.logger.warning(f"Rate limit (429) hit on {url}")
            time.sleep(10)
            raise requests.exceptions.RequestException("Rate limited")
            
        response.raise_for_status()
        return response

    def _apply_delay(self):
        """Human-like random delay"""
        time.sleep(random.uniform(self.delay_min, self.delay_max))
    
    
    