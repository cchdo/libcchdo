"""libcchdo.ctd.zip.exchange"""

import StringIO
import zipfile

import libcchdo
import libcchdo.model.datafile
from ..oden import oden


def read(self, handle):
    """How to read CTD ODEN files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for file in zip.namelist():
        if 'DOC' in file or 'README' in file:
            continue
        tempstream = StringIO.StringIO(zip.read(file))
        ctdfile = libcchdo.model.datafile.DataFile()
        oden(ctdfile).read(tempstream)
        self.datafile.files.append(ctdfile)
        tempstream.close()
    zip.close()

# OMIT writer
