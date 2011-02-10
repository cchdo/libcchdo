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


_HOST = {
    'cchdo': 'cchdo.ucsd.edu',
    'goship': 'goship.ucsd.edu',
}


_DBS = {
    'cchdo_data': S.engine.url.URL(
        _DRIVER['SQLITE'], None, None, None,
        database=config.get_option('db', 'cache')),
    'cchdo': S.engine.url.URL(
         _DRIVER['MYSQL'], 'cchdo_web', '((hd0hydr0d@t@', _HOST['cchdo'],
         database='cchdo'),
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
    return _connect(_DBS['cchdo_data'])


def cchdo():
    """Connect to CCHDO's database"""
    return _connect(_DBS['cchdo'])


@memoize
def sessionmaker(engine):
    return S.orm.sessionmaker(bind=engine)


def session(engine):
    return sessionmaker(engine)()
