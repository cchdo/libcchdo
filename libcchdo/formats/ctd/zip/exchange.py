import re
from tempfile import NamedTemporaryFile

from libcchdo.log import LOG
from libcchdo.model.datafile import DataFile
from libcchdo.formats import zip as Zip
from libcchdo.formats.zip import read as zip_read
from libcchdo.formats.ctd import exchange as ctdex
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = ['ct1.zip']


def get_filename(basename):
    """Return the filename for this format given a base filename.

    This is a basic implementation using filename extensions.

    """
    return get_filename_fnameexts(basename, _fname_extensions)


def is_filename_recognized(fname):
    """Return whether the given filename is a match for this file format.

    This is a basic implementation using filename extensions.

    """
    return is_filename_recognized_fnameexts(fname, _fname_extensions)


def is_file_recognized(fileobj):
    """Return whether the file is recognized based on its contents.

    This is a basic non-implementation.

    """
    return is_file_recognized_fnameexts(fileobj, _fname_extensions)


def read(self, handle, retain_order=False, header_only=False):
    """How to read CTD Exchange files from a Zip.

    The original filenames for each CTD file are included as the global
    _FILENAME on each individual CTD file.

    """
    def is_fname_ok(fname):
        if '.csv' not in fname:
            return False
        if fname.find('/') > -1:
            LOG.critical(('CTD Exchange Zip files should not contain '
                          'directories. Offending file name: %s') % fname)
            raise ValueError('CTD Exchange Zip files should not contain '
                             'directories. Please ensure you gave a CTD '
                             'Exchange Zip file to be read.')
        return True

    def reader(dfile, fileobj, retain_order, header_only):
        ctdex.read(dfile, fileobj, retain_order, header_only)
        dfile.globals['_FILENAME'] = fileobj.name
        
    zip_read(self, handle, is_fname_ok, reader, retain_order, header_only)


def write(self, handle):
    """How to write CTD Exchange files to a Zip."""
    Zip.write(self, handle, ctdex, ctdex.get_datafile_filename)
