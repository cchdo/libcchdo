"""CCHDO format for COARDS compliant netCDF files for CTD."""
from libcchdo.formats.ctd import netcdf as ctdnc
from libcchdo.formats import zip_netcdf as zipnc


def read(self, handle):
    """How to read CTD NetCDF files from a Zip."""
    zipnc.read(self, handle, ctdnc)


def write(self, handle):
    """How to write CTD NetCDF files to a Zip."""
    zipnc.write(self, handle, 'ctd', ctdnc, zipnc.get_identifier_ctd)
