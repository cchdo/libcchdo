"""
This library provides a format-less data model for the CCHDO and a centralized
place to put ways to read and write from it. Said format-less data model is
based on DataFiles which have Columns that are associated with Parameters. From
the data model, the user may write out to a database model of the data or read
in more data and mash it together or write it out in a different format. When
it is said the data is format-less, it is actually in a neutral format that
lets it be manipulated easily into other formats.

Reading/writing data to files
=============================
Data is represented internally as described in the model package. Currently
the main model used is datafile. Refer to the formats package for more with 
regard to file formats.

Internal Data Specification
===========================
Any unreported values must be represented as None. This includes -9, -999.000,
unspecified dates, times, etc.

Known unknown parameters have mnemonics that start with '_'. e.g. MAX PRESSURE
exists in certain files but there is no parameter defined for it. By prefixing
MAX_PRESSURE with a '_', the library will not retrive the parameter definition
from the database (there is none anyway).
"""

from libcchdo import config
from libcchdo.log import LOG
from libcchdo.util import StringIO, pyStringIO, memoize, get_library_abspath


__version__ = "0.7.1"


# Database cache for parameters will be ensured
check_cache = True


import libcchdo.formats


# Nice constants


RADIUS_EARTH = 6371.01 #km
