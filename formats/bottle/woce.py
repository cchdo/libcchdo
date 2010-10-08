import datetime
import re

from ... import fns
from ...model import datafile
from .. import woce


def read(self, handle):
    '''How to read a Bottle WOCE file.'''
    # Read Woce Bottle header
    try:
        stamp_line = handle.readline()
        parameters_line = handle.readline()
        units_line = handle.readline()
        asterisk_line = handle.readline()
        self.globals['header'] += '\n'.join(
            [stamp_line, parameters_line, units_line, asterisk_line])
    except Exception, e:
        raise ValueError('Malformed WOCE header in WOCE Bottle file: %s' % e)
    # Get stamp
    stamp = re.compile('EXPOCODE\s*([\w/]+)\s*WHP.?ID\s*([\w/]+(,[\w/]+)*)\s*CRUISE DATES\s*(\d{6}) TO (\d{6})\s*(\d{8}\w+)')
    m = stamp.match(stamp_line)
    if m:
        self.globals['EXPOCODE'] = m.group(1)
        self.globals['SECT_ID'] = fns.strip_all(m.group(2).split(','))
        self.globals['_BEGIN_DATE'] = m.group(4)
        self.globals['_END_DATE'] = m.group(5)
        self.globals['stamp'] = m.groups()[-1] # XXX
    else:
        raise ValueError(("Expected ExpoCode, SectIDs, dates, and a stamp. "
                          "Invalid WOCE record 1."))
    # Validate the parameter line
    if 'STNNBR' not in parameters_line or 'CASTNO' not in parameters_line:
        raise ValueError('Expected STNNBR and CASTNO in parameters record')
    woce.read_data(self, handle, parameters_line,
                                    units_line, asterisk_line)
    try:
        self.columns['DATE']
    except KeyError:
        self.columns['DATE'] = datafile.Column('DATE')
        self.columns['DATE'].values = [None] * len(self) # XXX
    try:
        self.columns['TIME']
    except KeyError:
        self.columns['TIME'] = datafile.Column('TIME')
        self.columns['TIME'].values = [None] * len(self)

    woce.fuse_datetime(self)
    
    self.check_and_replace_parameters()


def write(self, handle):
    '''How to write a Bottle WOCE file.'''

    #datetimes = self.columns["_DATETIME"].values[:]
    #BEGIN_DATE = 0
    #END_DATE = 0
    #if any(datetimes):
    #    usable_datetimes = filter(None, datetimes)
    #    BEGIN_DATE = min(usable_datetimes)
    #    END_DATE = max(usable_datetimes)
    #del self.columns["_DATETIME"]

    #handle.write("EXPOCODE %-s WHP-ID %-s CRUISE DATES %06d TO %06d %-s\n" %
    #        (self.globals["EXPOCODE"],
    #         self.globals["SECT_ID"][0],
    #         BEGIN_DATE,
    #         END_DATE,
    #         self.globals['stamp']))
    #woce.write_data(self, handle)
    return NotImplementedError("Not to be used, nitwit!")
