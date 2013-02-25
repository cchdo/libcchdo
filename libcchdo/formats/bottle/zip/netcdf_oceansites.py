from libcchdo.formats.bottle import netcdf_oceansites as ncos
from libcchdo.formats.netcdf_oceansites import write_zip_factory


# TODO maybe?
#def read(self, handle):
#    """How to read Bottle NetCDF OceanSITES files from a Zip."""
#    pass


write = write_zip_factory(ncos)
