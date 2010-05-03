"""libcchdo.formats.bottle.exchange"""

from sys import path
path.insert(0, '/'.join(path[0].split('/')[:-1]))
import re
import datetime

import libcchdo
from format import format

class exchange(format):
  
    def read(self, handle):
        '''How to read a Bottle Exchange file.'''
        df = self.datafile
        # Read identifier and stamp
        stamp = re.compile('BOTTLE,(\d{8}\w+)')
        m = stamp.match(handle.readline())
        if m:
            df.stamp = m.group(1)
        else:
            raise ValueError(("Expected identifier line with stamp "
                              "(e.g. BOTTLE,YYYYMMDDdivINSwho)"))
        # Read comments
        l = handle.readline()
        while l and l.startswith('#'):
            df.header += l
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
  
        df.create_columns(columns, units)
  
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
                                                    len(df)+1))
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
                        df.columns[column[:-7]].flags_woce.append(value)
                    except KeyError:
                        warn(("Flag WOCE column exists for parameter %s but "
                              "parameter column does not exist.") % column[:-7])
                elif column.endswith('_FLAG_I'):
                    try:
                        df.columns[column[:-7]].flags_igoss.append(value)
                    except KeyError:
                        warn(("Flag IGOSS column exists for parameter %s but "
                              "parameter column does not exist.") % column[:-7])
                else:
                    df.columns[column].append(value)
            l = handle.readline().strip()
        # Format all data to be what it is
        df.columns['LATITUDE'].values = map(float,
                                            df.columns['LATITUDE'].values)
        df.columns['LONGITUDE'].values = map(float,
                                             df.columns['LONGITUDE'].values)
        try:
            df.columns['DATE']
        except KeyError:
            df.columns['DATE'] = libcchdo.Column('DATE')
            df.columns['DATE'].values = ['0000-00-00'] * len(df)
        try:
            df.columns['TIME']
        except KeyError:
            df.columns['TIME'] = libcchdo.Column('TIME')
            df.columns['TIME'].values = ['0000'] * len(df)
        df.columns['_DATETIME'] = libcchdo.Column('_DATETIME')
        for d,t in zip(df.columns['DATE'].values, df.columns['TIME'].values):
            df.columns['_DATETIME'].append(datetime.datetime.strptime(
                '%04d%04d' % (int(d), int(t)), '%Y%m%d%H%M'))
        del df.columns['DATE']
        del df.columns['TIME']
  
    #def write(self, handle): TODO
    #  '''How to write a Bottle Exchange file.'''
