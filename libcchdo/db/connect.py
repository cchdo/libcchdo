"""Abstractions for SQLAlchemy connections.

"""

import os

import sqlalchemy as S
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL


from libcchdo import config
from libcchdo.util import memoize


_DRIVER = {
    'PG': 'postgresql',
    'MYSQL': 'mysql',
    'SQLITE': 'sqlite',
}


# Internal connection abstractions


@memoize
def _connect(url):
    """Create an engine for the given sqlalchemy url with default settings.

    Args:
        url - an sqlalchemy.engine.url.URL
    Returns:
        an engine

    """
    return S.create_engine(url)


# Public interface connections


@memoize
def cchdo_data():
    """Connect to cchdo_data.

    This is the sqlite database cache of parameters.

    """
    db_url = config.get_option('db', 'cache')
    url = URL(_DRIVER['SQLITE'], None, None, None, database=db_url)
    return _connect(url)


@memoize
def cchdo():
    """Connect to CCHDO's MySQL database."""
    cred = config.get_db_credentials_cchdo()
    url = URL(_DRIVER['MYSQL'], cred[0], cred[1], cred[2], database=cred[3])
    return _connect(url)


@memoize
def Sessionmaker(engine, **kwargs):
    return sessionmaker(bind=engine)


def session(engine, **kwargs):
    return Sessionmaker(engine)(**kwargs)
