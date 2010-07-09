"""libcchdo.formats.bottle.exchange"""

from sys import path
path.insert(0, '/'.join(path[0].split('/')[:-1]))
import re
import datetime

import libcchdo

def read(self, handle):
    '''How to read a Bottle Exchange file.'''
    # Read identifier and stamp
    stamp = re.compile('BOTTLE,(\d{8}\w+)')
    m = stamp.match(handle.readline())
    if m:
        self.stamp = m.group(1)
    else:
        raise ValueError(("Expected identifier line with stamp "
                          "(e.g. BOTTLE,YYYYMMDDdivINSwho)"))
    # Read comments
    l = handle.readline()
    while l and l.startswith('#'):
        self.header += l
        l = handle.readline()
    # Read columns and units
    columns = l.strip().split(',')
    units = handle.readline().strip().split(',')
    
    # Check columns and units to match length
    if len(columns) is not len(units):
        raise ValueError(("Expected as many columns as units in file. "
                          "Found %d columns and %d units.") % (len(columns),
                                                               len(units)))

    # Check for unique identifer
    identifier = []
    if 'EXPOCODE' in columns and \
       'STNNBR' in columns and \
       'CASTNO' in columns:
        identifier = ['STNNBR', 'CASTNO']
        if 'SAMPNO' in columns:
            identifier.append('SAMPNO')
            if 'BTLNBR' in columns:
                identifier.append('BTLNBR')
        elif 'BTLNBR' in columns:
            identifier.append('BTLNBR')
        else:
            raise ValueError(("No unique identifer found for file. "
                              "(STNNBR,CASTNO,SAMPNO,BTLNBR),"
                              "(STNNBR,CASTNO,SAMPNO),"
                              "(STNNBR,CASTNO,BTLNBR)"))

    self.create_columns(columns, units)

    # Read data
    l = handle.readline().strip()
    while l:
        if l == 'END_DATA': break
        values = l.split(',')
        
        # Check columns and values to match length
        if len(columns) is not len(values):
            raise ValueError(("Expected as many columns as values in file. "
                              "Found %d columns and %d values at "
                              "data line %d" % (len(columns), len(values),
                                                len(self)+1)))
        for column, raw in zip(columns, values):
            value = raw.strip()
            if libcchdo.out_of_band(value):
                value = float('nan')
            try:
                value = float(value)
            except:
                pass
            if column.endswith('_FLAG_W'):
                try:
                    self.columns[column[:-7]].flags_woce.append(int(value))
                except KeyError:
                    warn(("Flag WOCE column exists for parameter %s but "
                          "parameter column does not exist.") % column[:-7])
            elif column.endswith('_FLAG_I'):
                try:
                    self.columns[column[:-7]].flags_igoss.append(int(value))
                except KeyError:
                    warn(("Flag IGOSS column exists for parameter %s but "
                          "parameter column does not exist.") % column[:-7])
            else:
                self.columns[column].append(value)
        l = handle.readline().strip()
    # Format all data to be what it is
    self.columns['LATITUDE'].values = map(float,
                                        self.columns['LATITUDE'].values)
    self.columns['LONGITUDE'].values = map(float,
                                         self.columns['LONGITUDE'].values)
    try:
        self.columns['DATE']
    except KeyError:
        self.columns['DATE'] = libcchdo.Column('DATE')
        self.columns['DATE'].values = ['0000-00-00'] * len(self)
    try:
        self.columns['TIME']
    except KeyError:
        self.columns['TIME'] = libcchdo.Column('TIME')
        self.columns['TIME'].values = ['0000'] * len(self)
    self.columns['_DATETIME'] = libcchdo.Column('_DATETIME')
    for d,t in zip(self.columns['DATE'].values, self.columns['TIME'].values):
        self.columns['_DATETIME'].append(datetime.datetime.strptime(
            '%04d%04d' % (int(d), int(t)), '%Y%m%d%H%M'))
    del self.columns['DATE']
    del self.columns['TIME']

def write(self, handle): #TODO
    '''How to write a Bottle Exchange file.'''
    raise NotImplementedError
