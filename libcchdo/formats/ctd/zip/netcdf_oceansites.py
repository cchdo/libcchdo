from libcchdo.formats.ctd import netcdf_oceansites as ncos
from libcchdo.formats.netcdf_oceansites import write_zip_factory


#def read(self, handle):
#    """How to read CTD NetCDF OceanSITES files from a Zip."""


write = write_zip_factory(ncos)
