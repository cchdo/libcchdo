'''Abstractions for SQLAlchemy connections'''

import inspect
import os

import sqlalchemy as S
import sqlalchemy.orm


from .. import memoize


_DRIVER = {
    'PG': 'postgresql',
    'MYSQL': 'mysql',
    'SQLITE': 'sqlite',
}


_HOST = {
    'cchdo': 'cchdo.ucsd.edu',
    'goship': 'goship.ucsd.edu',
    'watershed': 'watershed.ucsd.edu',
}


_DB_MODULE_PATH = os.path.split(inspect.currentframe().f_code.co_filename)[0]


_DB_LIBRARY_FILE = 'cchdo_data.db'


_DBS = {
    'cchdo_data': S.engine.url.URL(
        _DRIVER['SQLITE'], None, None, None,
        database=os.path.join(_DB_MODULE_PATH, _DB_LIBRARY_FILE)),
    #'cchdo': S.engine.url.URL(
    #     _DRIVER['MYSQL'], 'cchdo_server', '((hd0hydr0d@t@', _HOST['cchdo'],
    #     database='cchdo'),
    'cchdo': S.engine.url.URL(
        _DRIVER['MYSQL'], 'jfields', 'c@keandc00kies', _HOST['cchdo'],
        database='cchdo', query={'charset': 'utf8'}),
    'watershed': S.engine.url.URL(
        _DRIVER['MYSQL'], 'jfields', 'c@keandc00kies', _HOST['watershed'],
        database='cchdo', query={'charset': 'utf8'}),
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
    #return _connect(_DBS['watershed'])


@memoize
def sessionmaker(engine):
    return S.orm.sessionmaker(bind=engine)


def session(engine):
    return sessionmaker(engine)()
