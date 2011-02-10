from ... import fns


#def read(self, handle):

def write(self, handle):
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
