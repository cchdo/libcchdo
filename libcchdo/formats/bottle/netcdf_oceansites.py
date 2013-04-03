from tempfile import NamedTemporaryFile

from libcchdo.formats.netcdf_oceansites import *


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
