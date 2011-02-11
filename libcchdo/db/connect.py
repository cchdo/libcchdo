"""
Abstractions for SQLAlchemy connections
"""

import os

import sqlalchemy as S
import sqlalchemy.orm


from .. import config
from .. import memoize


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


def cchdo_data():
    """Connect to cchdo_data"""
    url = S.engine.url.URL(
        _DRIVER['SQLITE'], None, None, None,
        database=config.get_option('db', 'cache'))
    return _connect(url)


@memoize
def cchdo():
    """Connect to CCHDO's database"""
    cred = config.get_db_credentials_cchdo()
    url = S.engine.url.URL(_DRIVER['MYSQL'], cred[0], cred[1], 'cchdo.ucsd.edu',
                           database='cchdo')
    return _connect(url)


@memoize
def sessionmaker(engine):
    return S.orm.sessionmaker(bind=engine)


def session(engine):
    return sessionmaker(engine)()
