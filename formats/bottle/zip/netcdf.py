"""libcchdo.formats.bottle.zip.netcdf"""

import os
import tempfile
import StringIO
import zipfile

from .. import netcdf


def read(self, handle):
    """How to read Bottle NetCDF files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for file in zip.namelist():
        if '.csv' not in file: continue
        tempstream = StringIO.StringIO(zip.read(file))
        bottlefile = DataFile()
        netcdf(bottlefile).read(tempstream)
        self.datafile.files.append(bottlefile)
        tempstream.close()
    zip.close()


def write(self, handle):
    """How to write Bottle NetCDF files to a Zip.

    The collection should already be split apart based on station cast.
    """
    # NetCDF libraries like to write to a file.
    # Work around that by giving temp dir.
    tempdir = tempfile.mkdtemp()

    for file in self.files:
        netcdf(file).write(tempdir)

    zip = zipfile.ZipFile(handle, 'w')
    for file in os.listdir(tempdir):
        fullpath = os.path.join(tempdir, file)
        zip.write(fullpath)
        os.remove(fullpath)
    os.rmdir(tempdir)
    zip.close()
