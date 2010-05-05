'''libcchdo.common.nav'''

import libcchdo


#def read(self, handle):

def write(self, handle):
    columns = self.datafile.columns
    dates = map(lambda d: d.strftime('%Y-%m-%d'), columns['_DATETIME'].values)
    try:
        codes = columns['_CODE']
    except KeyError, e:
        codes = ['BO'] * len(self.datafile)
    coords = zip(columns['LONGITUDE'].values, columns['LATITUDE'].values,
                 columns['STNNBR'].values, dates, codes)
    nav = libcchdo.uniquify(map(
        lambda coord: '%3.3f\t%3.3f\t%d\t%s\t%s\n' % coord, coords))
    handle.write(''.join(nav))
