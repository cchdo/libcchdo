from math import ceil

from libcchdo.formats import frcsv
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)
from libcchdo.algorithms.depth import depth_unesco
from libcchdo.model.datafile import DataFile
from libcchdo.model.convert.datafile_to_datafilecollection import split_on_cast
from libcchdo.log import LOG


_fname_extensions = ['.csv']


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


def read(dfc, fileobj):
    dfile = DataFile()
    retval = frcsv.read(dfile, fileobj, 'ctd')
    split_dfc = split_on_cast(dfile)
    dfc.files = split_dfc.files

    # Convert header columns to globals
    global_headers = [
        'EXPOCODE', 'STNNBR', 'CASTNO', '_DATETIME', 'LATITUDE', 'LONGITUDE']
    for dfile in dfc.files:
        for header in global_headers:
            value = dfile[header][0]
            if type(value) == int:
                value = str(value)
            dfile.globals[header] = value
            del dfile[header]

        # Arbitrarily set SECT_ID to blank
        dfile.globals['SECT_ID'] = ''

        # Take largest depth value and set as bottom depth
        try:
            depth = max(dfile['DEPTH'])
        except KeyError:
            depth = depth_unesco(max(dfile['CTDPRS']), dfile.globals['LATITUDE'])
        dfile.globals['DEPTH'] = ceil(depth)

    return retval
