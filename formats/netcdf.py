'''libcchdo.formats.netcdf'''


try:
    from netCDF3 import Dataset
except ImportError, e:
    raise ImportError('%s\n%s' % (e,
        ("You should get netcdf4-python from http://code.google.com/p/"
         "netcdf4-python and install the NetCDF 3 module as directed by the "
         "README.")))


QC_SUFFIX = '_QC'
FILE_EXTENSION = 'nc'


def _pad_station_cast(x):
    if type(x) is float: 
        x = int(x)
    return str(x).rjust(5, '0')


def get_filename(expocode, station, cast):
    station = _pad_station_cast(station)
    cast = _pad_station_cast(cast)
    return '%s.%s' % ('_'.join((expocode, station, cast, 'hy1')),
                      FILE_EXTENSION, )


