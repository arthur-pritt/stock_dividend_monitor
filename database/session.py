from sqlalchemy.orm import(
    Session,
    sessionmaker
)
from contextlib import contextmanager
from database.connection import get_engine

#Reusable Session Factory
SessionLocal = sessionmaker(
    bind= get_engine(),
    autoflush=False,
    expire_on_commit=False,
)


@contextmanager
def get_session():
    """
    Provide a transactional scope around a series of operations.
    Handles commit on success, rollback on error, and sesison cleanup
    Create and return a new SQLAlchemy Session."""

    session=SessionLocal()

    try:
        yield session 
        session.commit() #commit if no excepitions are raised

    except Exception as e:
        session.rollback() #Roll back all staged changes if an error occurs
        raise e #Re-raise exception so caller/pipeline is aware of the failure

    finally:
        session.close()
