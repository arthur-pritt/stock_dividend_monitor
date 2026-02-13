"Configuration settings for stock dividend monitor"

import os
from pathlib import Path 


# Project root
PROJECT_ROOT = Path(__file__).parent.parent  # config is at root, so go up one

#=======EXTRACT SETTINGS======
#File paths and directories
DATA_DIR = PROJECT_ROOT /'data'
RAW_SUBDIR = 'raw'
RAW_FILENAME = 'nasdaq_100_list.csv'

# Or combine them:
RAW_DATA_PATH = DATA_DIR / RAW_SUBDIR / RAW_FILENAME