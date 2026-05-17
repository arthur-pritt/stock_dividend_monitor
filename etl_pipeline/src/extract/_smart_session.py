
import requests
import random
import time 
import logging 
from tenacity import retry, stop_after_attempt,wait_exponential,retry_if_exception_type
from curl_cffi import requests    


class RobustCurlSession:
    """Robust session using curl_cffi for better Yahoo Finance compatibility."""

    def __init__(
            self,
            impersonate: str = "chrome131",
            per_second: int=1,
            burst: int=5,
            delay_min: float = 0.8,
            delay_max: float = 2.5,
            max_retries: int = 5
    ):
        #Create the curl cffi session object
        self.session = requests.Session()

        #Setting/configuring the impersonation(core anti-blocking feature)
        self.session.impersonate = impersonate 

        #Rate limiting setting/configurations

        self.per_second= per_second
        self.burst= burst
        self.delay_min= delay_min
        self.delay_max= delay_max

        #Request counter for basic rate limiting/tracking
        self.request_count = 0
        self.last_request_time = time.time()

        #Setup logging
        self.logger = logging.getLogger(__name__)

        #Setting  defaut headers
        self.session.headers.update({
              'Accept':'text/html,application/xhtml+xml,application/json,*/*',
              'Accept-Language': 'en-US,en;q=0.9',
              'Referer':'https://finance.yahoo.com/',
              'DNT':'1',
              "Sec-Fetch-Mode":"navigate",
              "Sec-Fetch-Site":"same-origin"
        })

        self.max_retries=max_retries
        #REQUESR METHODS
    @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((
                requests.exceptions.RequestException,
                TimeoutError,
                OSError,
                ConnectionError,)))


    def get(self, url, params=None, **kwargs):
        """GET request with retry + delay"""
        self._apply_rate_limit()
        self.randomize_headers()
        response = self.session.get(url, params=params, **kwargs)

        if response.status_code == 429:
            self.logger.warning(f"Rate limit hit (429) on {url}. Sleeping...")
            time.sleep(10)
            raise requests.exceptions.RequestException("Rate limited")
        
        response.raise_for_status()
        return response 
    
    def post(self, url, data=None, json=None, **kwargs):
        
        self._apply_rate_limit()
        self.randomize_headers()
        response = self.session.post(url, data=data, json=json, **kwargs)
        return response 
    #Helper Methods
    
    def _apply_rate_limit(self):
        """Add random human-like delay + basic rate limiting."""
        self.request_count += 1

        #Add random human delay
        time.sleep(random.uniform(self.delay_min, self.delay_max))

        #Very basic rate limiting
        if self.request_count >= self.burst:
            elapsed = time.time() - self.last_request_time
            if elapsed < 1.0:
                sleep_time = 1.0 - elapsed + random.uniform(0.5, 1.5)
                time.sleep(sleep_time)
            self.request_count = 0
            self.last_request_time = time.time()

    def _randomize_headers(self):
        """Rotating some headers for extra stealth"""
        user_agents=[
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        ]
        self.session.headers['User-Agent']=random.choice(user_agents)

            
            



       