'''libcchdo.bottle.woce'''

import datetime

import libcchdo.formats.woce


def read(self, handle):
    '''How to read a Bottle WOCE file.'''
    # Read Woce Bottle header
    try:
        stamp_line = handle.readline()
        parameters_line = handle.readline()
        units_line = handle.readline()
        asterisk_line = handle.readline()
        self.header+='\n'.join([stamp_line, parameters_line,
                                units_line, asterisk_line])
    except Exception, e:
        raise ValueError('Malformed WOCE header in WOCE Bottle file: %s' % e)
    # Get stamp
    stamp = compile('EXPOCODE\s*([\w/]+)\s*WHP.?ID\s*([\w/]+(,[\w/]+)*)\s*CRUISE DATES\s*(\d{6}) TO (\d{6})\s*(\d{8}\w+)')
    m = stamp.match(stamp_line)
    if m:
        self.globals['EXPOCODE'] = m.group(1)
        self.globals['SECT_ID'] = strip_all(m.group(2).split(','))
        self.globals['_BEGIN_DATE'] = m.group(3)
        self.globals['_END_DATE'] = m.group(4)
        self.stamp = m.group(5)
    else:
        raise ValueError(("Expected ExpoCode, SectIDs, dates, and a stamp. "
                          "Invalid WOCE record 1."))
    # Validate the parameter line
    if 'STNNBR' not in parameters_line or 'CASTNO' not in parameters_line:
        raise ValueError('Expected STNNBR and CASTNO in parameters record')
    self.read_WOCE_data(handle, parameters_line, units_line, asterisk_line)
    try:
        self.columns['DATE']
    except KeyError:
        self.columns['DATE'] = Column('DATE')
        self.columns['DATE'].values = ['0000-00-00'] * len(self)
    try:
        self.columns['TIME']
    except KeyError:
        self.columns['TIME'] = Column('TIME')
        self.columns['TIME'].values = ['0000'] * len(self)
    self.columns['_DATETIME'] = Column('_DATETIME')
    for d,t in zip(self.columns['DATE'].values,
                   self.columns['TIME'].values):
        self.columns['_DATETIME'].append(
            libcchdo.formats.woce.strptime_woce_date_time(int(d), int(t))
    del self.columns['DATE']
    del self.columns['TIME']


#def write(self, handle):
#    '''How to write a Bottle WOCE file.'''
#    raise NotImplementedError # TODO
