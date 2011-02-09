from __future__ import with_statement
import datetime
import tempfile
import tarfile

from .... import LOG
from ....model import datafile
from ...ctd import netcdf_andrex as nca


def read(self, handle):
    """How to read CTD NetCDF Andrex files from a tar.gz."""
    tar = tarfile.open(fileobj=handle)
    for file in tar.getmembers():
        if not file.name.endswith('.nc'):
        	continue
        tmpfile = tempfile.NamedTemporaryFile(prefix=file.name)
        tmpfile.write(tar.extractfile(file).read())
        tmpfile.flush()
        ctdfile = datafile.DataFile()
        nca.read(ctdfile, tmpfile)
        self.files.append(ctdfile)
        tmpfile.close()
    tar.close()
