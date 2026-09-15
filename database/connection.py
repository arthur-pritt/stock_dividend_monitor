import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

def _get_required_env(var_name: str) -> str:
    """
    Retrieve a required environment variable.
    Checks Streamlit secrets first (for cloud deployment),
    then falls back to the local environment / .env file.

    Raises:
        ValueError: if the environment variable is missing or empty.
    """
    value = None

    # Try Streamlit secrets first (only available when running under Streamlit)
    try:
        import streamlit as st
        if var_name in st.secrets:
            value = st.secrets[var_name]
    except Exception:
        pass

    # Fall back to local environment / .env file
    if not value:
        value = os.getenv(var_name)

    if not value:
        raise ValueError(
            f"Required environment variable '{var_name}' is not set."
        )

    return value


# Reading database configuration

db_port = _get_required_env("DB_PORT")
db_user = _get_required_env("DB_USER")
db_pass = _get_required_env("DB_PASSWORD")
db_name = _get_required_env("DB_NAME")
db_host = _get_required_env("DB_HOST")

# Building PostgreSQL connection URL
db_url = (f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

# Create ONE reusable engine for the entire application
_engine = create_engine(
    db_url,
    echo=False,
    pool_pre_ping=True
)

def get_engine() -> Engine:
    """
    Return the application's shared SQLAlchemy Engine.

    Returns:
        Engine: A reusable SQLAlchemy Engine Instance"""

    return _engine
    
    