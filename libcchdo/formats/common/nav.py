from libcchdo.fns import _decimal
from libcchdo.log import LOG
from libcchdo.model.navcoord import TabbedNavCoords, iter_coords


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
                LOG.warn(u'Coordinates could not be split on whitespace.')
                split_on = 'comma'
        elif split_on == 'comma':
            coords = l.split(',')
            if len(coords) < 2:
                LOG.warn(u'Coordinates could not be split on comma.')
                split_on = None
    if split_on is None:
        LOG.error(u'Coordinates could not be split using known schemes.')
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
