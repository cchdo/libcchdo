import StringIO
import zipfile

from ....model import datafile
from ..oden import oden


def read(self, handle):
    """How to read CTD ODEN files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for file in zip.namelist():
        if 'DOC' in file or 'README' in file:
            continue
        tempstream = StringIO.StringIO(zip.read(file))
        ctdfile = datafile.DataFile()
        oden(ctdfile).read(tempstream)
        self.datafile.files.append(ctdfile)
        tempstream.close()
    zip.close()

# OMIT writer
