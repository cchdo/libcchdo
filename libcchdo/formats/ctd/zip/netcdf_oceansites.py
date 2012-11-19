import datetime
import zipfile

from libcchdo import StringIO
from .. import netcdf_oceansites as nco
from ... import zip as Zip


#def read(self, handle):
#    """How to read CTD NetCDF OceanSITES files from a Zip."""


def write(self, handle, timeseries=None, timeseries_info={},
          version=nco.OCEANSITES_VERSIONS[-1]):
    """How to write CTD NetCDF OceanSITES files to a Zip."""
    zip = Zip.create(handle)
    for i, file in enumerate(self.files):
        tempstream = StringIO()
        nco.write(file, tempstream, timeseries, timeseries_info)
        info = zipfile.ZipInfo('%s.nc' % file.globals['OS_id'])
        dt = datetime.datetime.now()
        info.date_time = (dt.year, dt.month, dt.day,
                          dt.hour, dt.minute, dt.second)
        info.external_attr = 0644 << 16L
        info.compress_type = zipfile.ZIP_DEFLATED
        zip.writestr(info, tempstream.getvalue())
        tempstream.close()
    zip.close()
