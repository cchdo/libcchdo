import tarfile
from contextlib import closing

from libcchdo.log import LOG
from libcchdo.model.datafile import DataFile
from libcchdo.formats.ctd import bonusgoodhope as bgh


def read(self, handle):
    """How to read CTD Bonus Goodhope files from a TAR."""

    with tarfile.open(fileobj=handle) as tf:
        for member in tf.getmembers():
            ecp_file = tf.extractfile(member)
            ctdfile = DataFile()
            try:
                bgh.read(ctdfile, ecp_file)
            except ValueError:
                LOG.error(u'Failed on {0}'.format(member.name))
            self.files.append(ctdfile)
