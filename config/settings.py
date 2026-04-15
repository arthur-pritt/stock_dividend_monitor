"Configuration settings for stock dividend monitor"

from pathlib import Path 


# Project root
PROJECT_ROOT = Path(__file__).parent.parent  # config is at root, so go up one

#=======EXTRACT SETTINGS======
#File paths and directories
DATA_DIR = PROJECT_ROOT /'data'
RAW_SUBDIR = 'raw'
RAW_FILENAME = 'nasdaq_raw_list.csv'

# Or combine them:
RAW_DATA_PATH = DATA_DIR / RAW_SUBDIR / RAW_FILENAME

#Columns names
DATA_COLS = {
    "ticker":"symbol",
    "name":"name",
    "valuations":"market_cap"
}

#Internal column names

INTERNAL_COLS={
    "clean_name":"Name_clean",
    "match_score":"match_score",
    "trust":"trust_level"
}

#Patterns

CLEANING_PATTERNS =[
    r',.*$',                                     # Remove everything after a comma
    r'\..*$',                                    # Remove everything after a dot
    r'\b(corporation|corp)\b.*$',                # Remove Corp and everything after
    r'\b(common stock|ordinary shares?)\b',      # Remove share class noise
    r'\b\d+\.?\d*%?\b',                          # Remove numbers/percentages
]

# Standard abbreviations 
CLEANING_REPLACEMENTS = {
    r'\bcompany\b': 'co',
    r'\bincorporation\b': 'inc'
}

# Rules for identifying "Bad" symbols (Master List filter)
SYMBOL_EXCLUSION_REGEX = r'[/-]|\.WS'

# Trust Level Thresholds
THRESHOLDS = {
    "green": 90,
    "yellow": 70
}

# Human-Readable Labels
LABELS = {
    "verified": "Green: Verified",
    "review": "Yellow: Review Needed",
    "unknown": "Red: New/Unknown"
}

# Financial Aggregation Rules
MARKET_CAP_AGG = "max" 