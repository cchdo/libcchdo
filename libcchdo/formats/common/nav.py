from ... import fns
from ...model import datafile


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

    lons = self['LONGITUDE']
    lats = self['LATITUDE']

    for l in handle:
        lon, lat = map(fns._decimal, l.split())
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

