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
    Create and return a new SQLAlchemy Session."""

    session=SessionLocal()

    try:
        yield session 

    finally:
        session.close()
