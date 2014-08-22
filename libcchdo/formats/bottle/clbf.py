from logging import getLogger


log = getLogger(__name__)


from libcchdo.fns import _decimal
from libcchdo.model.datafile import Column
from libcchdo.formats import woce
from libcchdo.formats.exchange import (
    FLAG_ENDING_WOCE, FLAG_ENDING_IGOSS,
    read_identifier_line, read_comments, read_data, write_identifier,
    write_data, write_flagged_format_parameter_values, FILL_VALUE, END_DATA)
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = ['.clbf']


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


def read(self, fileobj):
    """How to read a .clbf file.

    """
    # There is not enough information so, split on whitespace
    columns = fileobj.readline().rstrip().split()


    self.check_and_replace_parameters()
