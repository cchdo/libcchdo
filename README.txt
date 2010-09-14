libcchdo README

libcchdo provides a format-less data model for the CCHDO and a centralized
place to put ways to read and write from it. Said format-less data model is
based on DataFiles which have Columns that are associated with Parameters. From
the data model, the user may write out to a database model of the data or read
in more data and mash it together or write it out in a different format. When
it is said the data is format-less, it is actually in a neutral format that
lets it be manipulated easily into other formats.

,------------------------------------------------------------------------------
- Changelog
2010-04-18 myshen Complete refactoring into modules.
2009-07-29 myshen Initial write-up.

,------------------------------------------------------------------------------
- Dependencies
netcdf4-python - http://code.google.com/p/netcdf4-python/
  Install the NetCDF 3 module as directed by the README
  Depends on:
    - netcdf
    - numpy - http://numpy.scipy.org/
sqlalchemy - http://sqlalchemy.org
  >=0.6.3

,------------------------------------------------------------------------------
- Testing
$ setup.py test
Every file under tests/ will be run as a python test battery.
