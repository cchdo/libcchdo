from libcchdo.util import StringIO
from libcchdo.model.datafile import DataFile
from libcchdo.formats import zip as Zip
from libcchdo.formats.ctd import oden


def read(self, handle):
    """How to read CTD ODEN files from a Zip."""
    zip = Zip.ZeroCommentZipFile(handle, 'r')
    for file in zip.namelist():
        if 'DOC' in file or 'README' in file:
            continue
        tempstream = StringIO(zip.read(file))
        ctdfile = DataFile()
        oden(ctdfile).read(tempstream)
        self.files.append(ctdfile)
        tempstream.close()
    zip.close()

# OMIT writer
