from tempfile import NamedTemporaryFile

from libcchdo.formats.netcdf_oceansites import *
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = ['_btl_os.zip', '_btl_oceansites.zip', '_nc_hyd_oceansites.zip']


def get_filename(basename):
    """Return the filename for this format given a base filename.

    This is a basic implementation using filename extensions.

    """
    return get_filename_fnameexts(basename, _fname_extensions)


def is_filename_recognized(fname):
    """Return whether the given filename is a match for this file format.

    This is a basic implementation using filename extensions.

    """
    return is_filename_recognized_fnameexts(fname, _fname_extensions)


def is_file_recognized(fileobj):
    """Return whether the file is recognized based on its contents.

    This is a basic non-implementation.

    """
    return is_file_recognized_fnameexts(fileobj, _fname_extensions)


#def read(self, handle): TODO
#    """How to read a Bottle NetCDF OceanSITES file."""


def write(self, handle, timeseries=None, timeseries_info={}, version=None):
    """How to write a Bottle NetCDF OceanSITES file.

    """
    # netcdf library wants to write its own files.
    tmp = NamedTemporaryFile()
    data_type = 'BTL'
    nc_file = create_oceansites_nc(self, tmp.name, data_type, version)
    write_columns(self, nc_file)
    write_timeseries_info_title_and_id(
        self, nc_file, data_type, timeseries, timeseries_info, version)
    nc_file.close()

    handle.write(tmp.read())
    tmp.close()
