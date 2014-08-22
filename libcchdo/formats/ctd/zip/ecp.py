import tarfile
from contextlib import closing
from logging import getLogger


log = getLogger(__name__)


from libcchdo.model.datafile import DataFile
from libcchdo.formats.ctd import ecp


def read(self, handle):
    """How to read CTD Bonus Goodhope files from a TAR."""

    with tarfile.open(fileobj=handle) as tf:
        for member in tf.getmembers():
            ecp_file = tf.extractfile(member)
            ctdfile = DataFile()
            try:
                ecp.read(ctdfile, ecp_file)
            except ValueError:
                log.error(u'Failed on {0}'.format(member.name))
            self.files.append(ctdfile)
