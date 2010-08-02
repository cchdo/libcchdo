"""libcchdo.formats.bottle.exchange"""

import re
import datetime

import libcchdo
import libcchdo.formats.woce


def read(self, handle):
    '''How to read a Bottle Exchange file.'''
    # Read identifier and stamp
    stamp = re.compile('BOTTLE,(\d{8}\w+)')
    m = stamp.match(handle.readline())
    if m:
        self.globals['stamp'] = m.group(1)
    else:
        raise ValueError(("Expected identifier line with stamp "
                          "(e.g. BOTTLE,YYYYMMDDdivINSwho)"))
    # Read comments
    l = handle.readline()
    self.globals['header'] = ''
    while l and l.startswith('#'):
        self.globals['header'] += l
        l = handle.readline()
    # Read columns and units
    columns = [x.strip() for x in l.strip().split(',')]
    units = [x.strip() for x in handle.readline().strip().split(',')]
    
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
            raise ValueError(
                ("No unique identifer found for file. "
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
                              "data line %d") % (len(columns), len(values),
                                                len(self) + 1))

        for column, raw in zip(columns, values):
            value = raw.strip()
            if libcchdo.fns.out_of_band(value):
                value = None
            try:
                value = float(value)
            except:
                pass
            if column.endswith('_FLAG_W'):
                try:
                    self.columns[column[:-7]].flags_woce.append(int(value))
                except KeyError:
                    libcchdo.warn(("Flag WOCE column exists for parameter %s "
                                   "but parameter column does not exist.") % \
                                  column[:-7])
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
    self.columns['LATITUDE'].values = map(
        float, self.columns['LATITUDE'].values)
    self.columns['LONGITUDE'].values = map(
        float, self.columns['LONGITUDE'].values)
    try:
        self.columns['DATE']
    except KeyError:
        self.columns['DATE'] = libcchdo.Column('DATE')
        self.columns['DATE'].values = [None] * len(self)
    try:
        self.columns['TIME']
    except KeyError:
        self.columns['TIME'] = libcchdo.Column('TIME')
        self.columns['TIME'].values = [None] * len(self)

    self.columns['_DATETIME'] = libcchdo.Column('_DATETIME')
    self.columns['_DATETIME'].values = [
        libcchdo.formats.woce.strptime_woce_date_time(*x) for x in zip(
            self.columns['DATE'].values, self.columns['TIME'].values)]
    del self.columns['DATE']
    del self.columns['TIME']

    self.check_and_replace_parameters()


def write(self, handle): #TODO
    '''How to write a Bottle Exchange file.'''
    handle.write('BOTTLE,%s%s\n' % \
        (datetime.datetime.now().strftime('%Y%m%d'), libcchdo.LIBVER))
    handle.write('# Original stamp: %s\n' % self.globals['stamp'])
    handle.write('# Original header:\n')
    handle.write(self.globals['header'])

    # Convert from internal data format to bottle exchange
    # Separate _DATETIME into DATE and TIME
    # TODO
    date = self.columns['DATE'] = libcchdo.Column('DATE')
    time = self.columns['TIME'] = libcchdo.Column('TIME')
    for dtime in self.columns['_DATETIME'].values:
        date.append(dtime.strftime('%Y%m%d'))
        time.append(dtime.strftime('%H%M'))
    del self.columns['_DATETIME']
    self.check_and_replace_parameters()

    columns = self.sorted_columns()
    flagged_parameter_names = []
    flagged_units = []
    flagged_formats = []
    flagged_columns = []

    for c in columns:
        param = c.parameter
        flagged_parameter_names.append(param.mnemonic_woce())
        flagged_units.append(param.units.mnemonic if param.units and \
            param.units.mnemonic else '')
        flagged_formats.append(param.format)
        flagged_columns.append(c.values)
        if c.is_flagged_woce():
            flagged_parameter_names.append(param.mnemonic_woce() + '_FLAG_W')
            flagged_units.append('')
            flagged_formats.append('%1d')
            flagged_columns.append(c.flags_woce)
        if c.is_flagged_igoss():
            flagged_parameter_names.append(param.mnemonic_woce() + '_FLAG_I')
            flagged_units.append('')
            flagged_formats.append('%1d')
            flagged_columns.append(c.flags_igoss)

    handle.write(','.join(flagged_parameter_names))
    handle.write('\n')
    handle.write(','.join(flagged_units))
    handle.write('\n')

    flagged_formats_columns = zip(flagged_formats, flagged_columns)

    for i in range(len(self)):
        values = []

        for f, c in flagged_formats_columns:
            try:
                if c[i] is not None:
                    values.append(f % c[i])
                else:
                    values.append(f % -999.0)
            except Exception, e:
                libcchdo.warn('Arguments at %d:' % i)
                libcchdo.warn('\t%s and %s' % (f, c[i]))
                raise 

        handle.write(','.join(values))
        handle.write('\n')

    handle.write('END_DATA\n')
