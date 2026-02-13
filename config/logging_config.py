import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime 
from pathlib import Path


def setup_logging():
    #where logs will be saved
    LOGS_DIR = Path("logs")

    #Creates log directory if it doesn't exists
    os.makedirs(LOGS_DIR, exist_ok= True)

    #Log format defination

    log_format= "%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"
    date_format= "%Y-%m-%d %H:%M:%S"

    # File handler with daily rotation
    file_handler = TimedRotatingFileHandler(
        filename=LOGS_DIR / "app.log",
        when='midnight',        # Rotate at midnight
        interval=1,             # Every 1 day
        backupCount=30,         # Keep 30 days of logs
        encoding='utf-8'
    )
    file_handler.suffix = "%Y-%m-%d"  # Name pattern: app.log.2024-02-13
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))

    # Console handler (shows logs in terminal)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Set overall level
    
    # Remove existing handlers (prevents duplicates if called multiple times)
    logger.handlers.clear()
    
    # Attach both handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def get_logger(name: str = __name__):
    """Get a logger instance for a module
    
    Args:
        name: Usually __name__ to identify which module is logging
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)

if __name__ == "__main__":
    # Test the logging setup
    setup_logging()
    logger = get_logger(__name__)
    
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")





