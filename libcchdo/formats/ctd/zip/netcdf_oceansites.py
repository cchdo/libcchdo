import datetime
import StringIO
import zipfile

from .. import netcdf_oceansites as nco
from ... import zip as Zip


#def read(self, handle):
#    """How to read CTD NetCDF OceanSITES files from a Zip."""


def write(self, handle, timeseries=None, timeseries_info={},
          version=nco.OCEANSITES_VERSIONS[-1]):
    """How to write CTD NetCDF OceanSITES files to a Zip."""
    zip = Zip.create(handle)
    for i, file in enumerate(self.files):
        tempstream = StringIO.StringIO()
        nco.write(file, tempstream, timeseries, timeseries_info)
        timeseries_info = nco.pick_timeseries_or_timeseries_info(
            file, timeseries, timeseries_info)
        id = nco.file_and_timeseries_info_to_id(
            file, timeseries_info, version)
        info = zipfile.ZipInfo('%s.nc' % id)
        dt = datetime.datetime.now()
        info.date_time = (dt.year, dt.month, dt.day,
                          dt.hour, dt.minute, dt.second)
        info.external_attr = 0644 << 16L
        info.compress_type = zipfile.ZIP_DEFLATED
        zip.writestr(info, tempstream.getvalue())
        tempstream.close()
    zip.close()
