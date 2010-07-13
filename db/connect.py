"""libcchdo.db.connect"""

DB_CREDENTIALS = {
    # Postgres
#    'goship(cchdo_data)': {'user': 'libcchdo', 'password': '((hdo0hydr0d@t@',
#                           'host': 'goship.ucsd.edu', 'database': 'cchdo_data'},
#    'goship(cchdotest)':  {'user': 'libcchdo', 'password': '((hd0hydr0d@t@',
#                           'host': 'goship.ucsd.edu', 'database': 'cchdotest'},
    # MySQL
#    'cchdo(cchdo)':       {'user': 'cchdo_server', 'passwd': '((hdo0hydr0d@t@',
#                           'host': 'cchdo.ucsd.edu', 'db': 'cchdo'},
    'cchdo(cchdo)':       {'user': 'jfields', 'passwd': 'c@keandc00kies',
                           'host': 'cchdo.ucsd.edu', 'db': 'cchdo'},
    'watershed(cchdo)':   {'user': 'jfields', 'passwd': 'c@keandc00kies',
                           'host': 'watershed.ucsd.edu', 'db': 'cchdo'},
}


# Internal connection abstractions


_CONNECTION_CACHE = {}


def _connect(module, error, **credentials):
    """Connect to a given Python DB-API compliant database.
       Args:
           module - the DB-API module to use
           error - the module specific error to consider as a database error
           credentials - a dictionary of credentials to give to the module's
                         connect()
       Returns:
           an active DB connection using the given arguments
    """
    key = str(credentials)
    if key in _CONNECTION_CACHE:
        conn = _CONNECTION_CACHE[key]
        try:
            if conn.open:
                return conn
        except Error, e:
            # The connection in the cache has been closed. Reopen it.
            pass
        
    try:
        conn = module.connect(**credentials)
        _CONNECTION_CACHE[key] = conn
        return conn
    except error, e:
        raise IOError("Database error: %s" % e)


def _pg(**credentials):
    """Connect to a given postgresql database"""
    try:
        import pgdb
    except ImportError, e:
        raise ImportError('%s\n%s' % (e,
            ("You should get pygresql from http://www.pygresql.org/readme.html"
             "#where-to-get. You will need Postgresql with server binaries "
             "installed already.")))
    return _connect(pgdb, pgdb.Error, **credentials)


def _mysql(**credentials):
    """Connect to a given MySQL database"""
    try:
        import MySQLdb
    except ImportError, e:
        raise ImportError('%s\n%s' % (e,
            ("You should get MySQLdb from http://sourceforge.net/projects/"
             "mysql-python. You will need MySQL with server binaries "
             "installed already.")))
    return _connect(MySQLdb, MySQLdb.Error, **credentials)


# Public interface connections

def cchdotest():
    """Connect to cchdotest"""
    return _pg(**DB_CREDENTIALS['goship(cchdotest)'])


def cchdo_data():
    """Connect to cchdo_data"""
    return _pg(**DB_CREDENTIALS['goship(cchdo_data)'])


def cchdo():
    """Connect to CCHDO's database"""
    return _mysql(**DB_CREDENTIALS['cchdo(cchdo)'])
    #return _mysql(**DB_CREDENTIALS['watershed(cchdo)'])
