import datetime
import StringIO
import zipfile

from .. import netcdf_oceansites as nco


#def read(self, handle):
#    """How to read CTD NetCDF OceanSITES files from a Zip."""


def write(self, handle, timeseries=None, timeseries_info={}):
    """How to write CTD NetCDF OceanSITES files to a Zip."""
    zip = zipfile.ZipFile(handle, 'w')
    for i, file in enumerate(self.files):
        tempstream = StringIO.StringIO()
        nco.write(file, tempstream, timeseries, timeseries_info)
        info = zipfile.ZipInfo('OS_%05d.nc' % i)
        dt = datetime.datetime.now()
        info.date_time = (dt.year, dt.month, dt.day,
                          dt.hour, dt.minute, dt.second)
        info.external_attr = 0644 << 16L
        zip.writestr(info, tempstream.getvalue())
        tempstream.close()
    zip.close()
