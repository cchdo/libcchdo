from logging import getLogger


log = getLogger(__name__)


from libcchdo.fns import _decimal
from libcchdo.model.navcoord import TabbedNavCoords, iter_coords
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = [
    '_tracks.txt', 'tracks.txt', '_na.txt', 'na.txt', '_nav.txt', 'nav.txt',
    '.nav']


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
    """How to read a CCHDO tracks file."""
    self.create_columns(['LONGITUDE', 'LATITUDE'])

    lons = self['LONGITUDE'].values
    lats = self['LATITUDE'].values

    l = handle.readline()
    split_on = 'space'
    coords = []
    while len(coords) < 2 and split_on is not None:
        if split_on == 'space':
            coords = l.split()
            if len(coords) < 2:
                log.warn(u'Coordinates could not be split on whitespace.')
                split_on = 'comma'
        elif split_on == 'comma':
            coords = l.split(',')
            if len(coords) < 2:
                log.warn(u'Coordinates could not be split on comma.')
                split_on = None
    if split_on is None:
        log.error(u'Coordinates could not be split using known schemes.')
        return

    if split_on == 'space':
        coords = l.split()
    elif split_on == 'comma':
        coords = l.split(',')
    lon, lat = map(_decimal, coords[:2])
    lons.append(lon)
    lats.append(lat)

    for l in handle:
        if split_on == 'space':
            coords = l.split()
        elif split_on == 'comma':
            coords = l.split(',')
        lon, lat = map(_decimal, coords[:2])
        lons.append(lon)
        lats.append(lat)


def write(self, handle):
    """How to write a CCHDO nav file."""
    def print_coords(expocode, coords):
        handle.write(str(coords) + '\n')
        
    iter_coords(self, TabbedNavCoords, print_coords)
