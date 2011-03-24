from ... import fns
from ...model import datafile


#def read(self, handle):

def write(self, handle):
    """ How to write a CCHDO nav file.
    There are two possibilities for self: it can either be a DataFile or
    DataFileCollection.
    """
    if isinstance(self, datafile.DataFile):
        dates = map(lambda d: d.strftime('%Y-%m-%d'), self['_DATETIME'].values)
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

