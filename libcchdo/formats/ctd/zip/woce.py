import zipfile
from traceback import print_exc

from .... import LOG, StringIO
from ....model import datafile
from .. import woce


def read(self, handle):
    """How to read CTD WOCE files from a Zip."""
    try:
        zip = zipfile.ZipFile(handle, 'r')
    except zipfile.BadZipfile, e:
        LOG.info(
            'The zip file probably has trailing characters or has comment '
            'length that does not match. See '
            'http://hg.python.org/cpython/rev/cc3255a707c7/'
        )
        raise e

    for file in zip.namelist():
        if 'README' in file or 'DOC' in file: continue
        tempstream = StringIO(zip.read(file))
        ctdfile = datafile.DataFile()
        try:
            woce.read(ctdfile, tempstream)
        except Exception, e:
            LOG.info('Failed to read file %s in %s' % (file, handle))
            print_exc()
            raise e
        self.append(ctdfile)
        tempstream.close()
    zip.close()

#def write(self, handle): TODO
#    """How to write CTD WOCE files to a Zip."""
