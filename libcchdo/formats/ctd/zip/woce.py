import zipfile
from StringIO import StringIO as pyStringIO
try:
    from cStringIO import StringIO
except ImportError:
    StringIO = pyStringIO

from .... import LOG
from ....model import datafile
from .. import woce


def read(self, handle):
    """How to read CTD WOCE files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for file in zip.namelist():
        if 'README' in file or 'DOC' in file: continue
        tempstream = StringIO(zip.read(file))
        ctdfile = datafile.DataFile()
        try:
            woce.read(ctdfile, tempstream)
        except Exception, e:
            LOG.info('Failed to read file %s in %s' % (file, handle))
            raise e
        self.append(ctdfile)
        tempstream.close()
    zip.close()

#def write(self, handle): TODO
#    """How to write CTD WOCE files to a Zip."""
