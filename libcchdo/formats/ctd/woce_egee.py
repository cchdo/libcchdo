import re
from datetime import datetime

from libcchdo.fns import uniquify
from libcchdo.algorithms.depth import depth_unesco
from libcchdo.formats import woce
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)
from libcchdo.log import LOG


_fname_extensions = ['.ctd']


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


def read(self, handle):
    """How to read a CTD WOCE Egee file."""
    # Egee1
    egee = handle.readline().strip()
    # STRNBR CASTNO NO RECORDS
    line_cast_id = handle.readline()
    match_cast_id = re.match(
        '\s*STRNBR(.*)CASTNO(.*)NO.RECORDS(.*)', line_cast_id)
    try:
        cast_id = [xxx.strip() for xxx in match_cast_id.groups()]
        self.globals['STNNBR'] = cast_id[0]
        self.globals['CASTNO'] = cast_id[1]
        num_records = int(cast_id[2])
    except AttributeError:
        LOG.error(u'Unable to read station cast and number of records.')
    except TypeError:
        LOG.warn(u'Unable to determine number of data records.')
    # DATE
    line_date = handle.readline()
    try:
        line_date = line_date.split(':', 1)[1].strip()
        dtime = datetime.strptime(line_date, '%b %d %Y %H:%M:%S')
        self.globals['_DATETIME'] = dtime
    except IndexError:
        LOG.warn(u'Unable to determine date.')
    # blank
    handle.readline()
    # LATITUDE LONGITUDE
    line_coord = handle.readline()
    match_coord = re.match(
        '\s*LATITUDE:(.*)LONGITUDE:(.*)', line_coord)
    try:
        coord = [xxx.strip() for xxx in match_coord.groups()]
        lat_coords = coord[0].split()
        lng_coords = coord[1].split()
        self.globals['LATITUDE'] = woce.woce_lat_to_dec_lat(lat_coords)
        self.globals['LONGITUDE'] = woce.woce_lng_to_dec_lng(lng_coords)
    except AttributeError:
        LOG.error(u'Unable to read coordinates')
    
    parameters_line = handle.readline()
    units_line = handle.readline()
    asterisk_line = handle.readline()

    woce.read_data_egee(
        self, handle, parameters_line, units_line, asterisk_line)

    self.check_and_replace_parameters()

    self.globals['EXPOCODE'] = egee
    self.globals['SECT_ID'] = 'EGEE'
    self.globals['DEPTH'] = [
        int(depth_unesco(vvv, self.globals['LATITUDE']) or 0)
        for vvv in self['CTDPRS'].values][-1]


def write(self, handle):
    '''How to write a CTD WOCE Egee file.'''
    raise NotImplementedError()
