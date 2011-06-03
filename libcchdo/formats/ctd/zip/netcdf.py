from __future__ import with_statement
import datetime
import tempfile
import zipfile
import StringIO

from ....model import datafile
from ...ctd import netcdf
from ... import netcdf as nc
from ... import zip as Zip


def read(self, handle):
    """How to read CTD NetCDF files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for file in zip.namelist():
        if '.nc' not in file: continue
        tmpfile = tempfile.NamedTemporaryFile()
        tmpfile.write(zip.read(file))
        tmpfile.flush()
        ctdfile = datafile.DataFile()
        with open(tmpfile.name, 'r') as f:
            netcdf.read(ctdfile, f)
        self.files.append(ctdfile)
        tmpfile.close()
    zip.close()


def write(self, handle):
    """How to write CTD NetCDF files to a Zip."""
    station_i = 0
    cast_i = 0

    zip = Zip.create(handle)
    for file in self:
        temp = StringIO.StringIO()
        netcdf.write(file, temp)

        # Create a name for the file
        expocode = file.globals.get('EXPOCODE', 'UNKNOWN')
        station = file.globals.get('STNNBR')
        cast = file.globals.get('CASTNO')
        if station is None:
            station = station_i
            station_i += 1
        if cast is None:
            cast = cast_i
            cast_i += 1
        filename = nc.get_filename(expocode, station, cast, extension='ctd')

        zip.writestr(filename, temp.getvalue())
        temp.close()
    zip.close()
