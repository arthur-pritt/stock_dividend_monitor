import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

def _get_required_env(var_name: str)-> str:
    """
    Retrieve a required environment variable.
    
    Raises:
         valueerror: if the environment variable is missing or empty.
    """

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
db_url =(f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

#Create ONE reusable engine for the entire application
_engine = create_engine(
    db_url,
    echo= False,
    #pool_size = 5,
    pool_pre_ping=True,
    #max_overflow=10
)

def get_engine()-> Engine:
    """
    Return the application's shared SQLAlchemny Engine.
    
    Returns:
           Engine: A reusable SQLAlchemy Engine Instance"""
    
    return _engine
    
    