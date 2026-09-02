import random
import time 
from collections.abc import Callable
from typing import TypeVar 

from sqlalchemy.exc import OperationalError, TimeoutError

T = TypeVar("T")

RETRYABLE_EXCEPTIONS =(
    OperationalError,
    TimeoutError
)

def retry_operation(
        operation: Callable[[], T],
        max_attempts: int = 3,
        base_delay: float = 0.5,
        max_delay: float = 5.0,
        jitter: float = 0.2
)-> T:
    """
    Execute an operation and retry it when a retryable exception occurs.
    Max_attempts include initial attempt"""

    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except RETRYABLE_EXCEPTIONS:
            #No attempts remaining.
            if attempt == max_attempts:
                raise 

            #Exponential backoff.
            delay = min(
                base_delay * (2**(attempt-1)),
                max_delay
            )

            #Add jitter
            delay = random.uniform(
                0,
                delay * (1 + jitter),
            )

            time.sleep(delay)

        except Exception:
            #Anything not explicity classified
            #as retryable should fail immediately
            raise 