"""libcchdo.db.connect"""

DB_CREDENTIALS = {
    # Postgres
#    'goship(cchdo_data)': {'user': 'libcchdo', 'password': '((hdo0hydr0d@t@',
#                           'host': 'goship.ucsd.edu', 'database': 'cchdo_data'},
#    'goship(cchdotest)':  {'user': 'libcchdo', 'password': '((hd0hydr0d@t@',
#                           'host': 'goship.ucsd.edu', 'database': 'cchdotest'},
    # MySQL
    #'cchdo(cchdo)':       {'user': 'cchdo_server', 'passwd': '((hdo0hydr0d@t@',
    #                       'host': 'cchdo.ucsd.edu', 'db': 'cchdo'},
    'cchdo(cchdo)':       {'user': 'jfields', 'passwd': 'c@keandc00kies',
                           'host': 'cchdo.ucsd.edu', 'db': 'cchdo'},
    'watershed(cchdo)':   {'user': 'jfields', 'passwd': 'c@keandc00kies',
                           'host': 'watershed.ucsd.edu', 'db': 'cchdo'},
}

try:
    import pgdb
except ImportError, e:
    raise ImportError('%s\n%s' % (e,
        ("You should get pygresql from http://www.pygresql.org/readme.html"
         "#where-to-get. You will need Postgresql with server binaries "
         "installed already.")))

try:
    import MySQLdb
except ImportError, e:
    raise ImportError('%s\n%s' % (e,
        ("You should get MySQLdb from http://sourceforge.net/projects/"
         "mysql-python. You will need MySQL with server binaries "
         "installed already.")))


# Internal connection abstractions

def _connect(module, error, **credentials):
    """Connect to a given Python DB-API compliant database"""
    try:
        return module.connect(**credentials)
    except error, e:
        raise IOError("Database error: %s" % e)


def _pg(**credentials):
    """Connect to a given postgresql database"""
    return _connect(pgdb, pgdb.Error, **credentials)


def _mysql(**credentials):
    """Connect to a given MySQL database"""
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
