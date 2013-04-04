"""CCHDO format for COARDS compliant netCDF files for bottle."""
from libcchdo.formats.bottle import netcdf as btlnc
from libcchdo.formats import zip_netcdf as zipnc


def read(self, handle):
    """How to read Bottle NetCDF files from a Zip."""
    zipnc.read(self, handle, btlnc)


def write(self, handle):
    """How to write Bottle NetCDF files to a Zip.

    The collection should already be split apart based on station cast.

    """
    zipnc.write(self, handle, 'hy1', btlnc, zipnc.get_identifier_btl)
