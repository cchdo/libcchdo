"""libcchdo.formats.ctd.zip.netcdf"""

from __future__ import with_statement
import datetime
import tempfile
import zipfile

import libcchdo
import libcchdo.model.datafile
import libcchdo.formats.ctd.netcdf as netcdf


def read(self, handle):
    """How to read CTD NetCDF files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for file in zip.namelist():
        if '.nc' not in file: continue
        tmpfile = tempfile.NamedTemporaryFile()
        tmpfile.write(zip.read(file))
        tmpfile.flush()
        ctdfile = libcchdo.model.datafile.DataFile()
        with open(tmpfile.name, 'r') as f:
            netcdf.read(ctdfile, f)
        self.files.append(ctdfile)
        tmpfile.close()
    zip.close()


#def write(self, handle):
#    """How to write CTD NetCDF files to a Zip."""
