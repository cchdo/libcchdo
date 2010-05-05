"""libcchdo.ctd.zip.exchange"""

import StringIO
import zipfile

from ..oden import oden


def read(self, handle):
    """How to read CTD ODEN files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for file in zip.namelist():
        if 'DOC' in file or 'README' in file:
            continue
        tempstream = StringIO.StringIO(zip.read(file))
        ctdfile = libcchdo.DataFile()
        oden(ctdfile).read(tempstream)
        self.datafile.files.append(ctdfile)
        tempstream.close()
    zip.close()

# OMIT writer
