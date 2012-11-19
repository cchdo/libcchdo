from traceback import print_exc

from libcchdo import LOG, StringIO
from libcchdo.formats import zip as Zip
from libcchdo.model.datafile import DataFile
from .. import woce


def read(self, handle):
    """How to read CTD WOCE files from a Zip."""
    zip = Zip.ZeroCommentZipFile(handle, 'r')
    try:
        for file in zip.namelist():
            if 'README' in file or 'DOC' in file: continue
            tempstream = StringIO(zip.read(file))
            ctdfile = DataFile()
            try:
                woce.read(ctdfile, tempstream)
            except Exception, e:
                LOG.info('Failed to read file %s in %s' % (file, handle))
                print_exc()
                raise e
            self.append(ctdfile)
            tempstream.close()
    finally:
        zip.close()

#def write(self, handle): TODO
#    """How to write CTD WOCE files to a Zip."""
