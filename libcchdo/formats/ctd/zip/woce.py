import StringIO
import zipfile

from ....model import datafile
from .. import woce


def read(self, handle):
    """How to read CTD WOCE files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for file in zip.namelist():
        if 'README' in file or 'DOC' in file: continue
        tempstream = StringIO.StringIO(zip.read(file))
        ctdfile = datafile.DataFile()
        woce.read(ctdfile, tempstream)
        self.append(ctdfile)
        tempstream.close()
    zip.close()

#def write(self, handle): TODO
#    """How to write CTD WOCE files to a Zip."""
