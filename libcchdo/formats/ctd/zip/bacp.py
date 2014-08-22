from contextlib import closing

from libcchdo.formats.ctd import bacp
from libcchdo.formats.zip import read as zip_read


def read(self, handle):
    """How to read CTD BACp files from a zip."""

    def is_fname_ok(fname):
        return True
    zip_read(self, handle, is_fname_ok, bacp.read)
