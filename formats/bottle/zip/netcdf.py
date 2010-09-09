import os
import tempfile
import StringIO
import zipfile

import libcchdo
import libcchdo.formats.netcdf as nc
import libcchdo.formats.bottle.netcdf as botnc


def read(self, handle):
    """How to read Bottle NetCDF files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for file in zip.namelist():
        if '.csv' not in file:
            continue
        tempstream = StringIO.StringIO(zip.read(file))
        file = libcchdo.DataFile()
        botnc.read(file, tempstream)
        self.files.append(file)
        tempstream.close()
    zip.close()


def write(self, handle):
    """How to write Bottle NetCDF files to a Zip.

       The collection should already be split apart based on station cast.
    """
    station_i = 0
    cast_i = 0

    zip = zipfile.ZipFile(handle, 'w')

    for file in self.files:
        temp = StringIO.StringIO()
        botnc.write(file, temp)

        # Create a name for the file
        expocode = file.columns['EXPOCODE'][0] or 'UNKNOWN'
        station = file.columns['STNNBR'][0]
        cast = file.columns['CASTNO'][0]
        if station is None:
            station = station_i
            station_i += 1
        if cast is None:
            cast = cast_i
            cast_i += 1
        filename = nc.get_filename(expocode, station, cast)

        zip.writestr(filename, temp.getvalue())
        temp.close()
    zip.close()
