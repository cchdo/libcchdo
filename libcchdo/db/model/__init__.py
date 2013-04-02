"""Database models.

"""
from contextlib import contextmanager
from warnings import catch_warnings, simplefilter

from sqlalchemy import exc as sa_exc


@contextmanager
def ignore_sa_warnings():
    """Ignore SQLAlchemy warnings."""
    with catch_warnings():
        simplefilter('ignore', category=sa_exc.SAWarning)
        yield
