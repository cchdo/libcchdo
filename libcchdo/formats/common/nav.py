from ... import fns
from ...model import datafile
from libcchdo import LOG


#def read(self, handle):


def _dt_to_str(dt):
    """Return the string representation of a datetime.

    If no datetime is given, return an empty string.

    """
    try:
        return dt.strftime('%Y-%m-%d')
    except AttributeError:
        return ''


def read(self, handle):
    """How to read a CCHDO tracks file."""
    self.create_columns(['LONGITUDE', 'LATITUDE'])

    lons = self['LONGITUDE'].values
    lats = self['LATITUDE'].values

    l = handle.readline()
    split_on = 'space'
    coords = []
    while len(coords) < 2 and split_on != 'failed':
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
    lon, lat = map(fns._decimal, coords[:2])
    lons.append(lon)
    lats.append(lat)

    for l in handle:
        if split_on == 'space':
            coords = l.split()
        elif split_on == 'comma':
            coords = l.split(',')
        lon, lat = map(fns._decimal, coords[:2])
        lons.append(lon)
        lats.append(lat)
    

def write(self, handle):
    """ How to write a CCHDO nav file.
    There are three possibilities for self:
        1. DataFile
        2. SummaryFile
        3. DataFileCollection

    """
    if (    isinstance(self, datafile.DataFile) or
            isinstance(self, datafile.SummaryFile)):
        dates = [_dt_to_str(dt) for dt in self['_DATETIME'].values]
        try:
            codes = self['_CODE']
        except KeyError, e:
            codes = ['BO'] * len(self)
        coords = zip(self['LONGITUDE'].values, self['LATITUDE'].values,
                     self['STNNBR'].values, dates, codes)
        nav = fns.uniquify(map(
            lambda coord: '%3.3f\t%3.3f\t%s\t%s\t%s\n' % coord, coords))
        handle.write(''.join(nav))
    elif isinstance(self, datafile.DataFileCollection):
        coords = []
        for file in self:
            coords.append('\t'.join(map(str, (file.globals['LONGITUDE'],
                                              file.globals['LATITUDE']))))
        handle.write('\n'.join(coords))
    else:
        raise ArgumentError("Don't know how to write a nav file from that.")

