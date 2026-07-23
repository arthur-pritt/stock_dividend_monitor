

from database.connection import get_engine

def run_load_pipeline(data):
    #Retrieve the engine from connection.py
    engine = get_engine()

    with engine.connect() as conn:
        # Perform write/upsert operation
        pass 

