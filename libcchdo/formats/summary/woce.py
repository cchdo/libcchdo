import re
from datetime import datetime
from logging import getLogger


log = getLogger(__name__)


from libcchdo import config
from libcchdo.fns import int_or_none, uniquify
from libcchdo.formats import woce
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = ['su.txt', '.sum']


def get_filename(basename):
    """Return the filename for this format given a base filename.

    This is a basic implementation using filename extensions.

    """
    return get_filename_fnameexts(basename, _fname_extensions)


def is_filename_recognized(fname):
    """Return whether the given filename is a match for this file format.

    This is a basic implementation using filename extensions.

    """
    return is_filename_recognized_fnameexts(fname, _fname_extensions)


def is_file_recognized(fileobj):
    """Return whether the file is recognized based on its contents.

    This is a basic non-implementation.

    """
    return is_file_recognized_fnameexts(fileobj, _fname_extensions)


cast_type_codes = {
    'BIO': 'Biological or biooptical cast',
    'BUC': 'Surface bucket sample (not recommended)',
    'BOT': 'Small volume bottle cast only. No CTD',
    'CTD': 'CTD only, no water samples. Includes fast fish casts',
    'DRF': 'Drifter deployment',
    'FLT': 'Float deployment',
    'LVS': 'Large volume samples',
    'MOR': 'Mooring',
    'ROS': 'Rosette water sampler plus CTD',
    'XBT': 'Expendable bathythermograph',
    'XCP': 'Expendable current profiler',
    'XCT': 'Expendable CTD casts.',
    'UNK': 'Unknown',
    'USW': 'Surface soak or sample taken from uncontaminated sea water line',
}


time_event_codes = {
    'AT': 'Time bottles were acoustically tripped on large volume cast',
    'BE': ('Beginning of cast. BE, BO, and EN time, position, and depth '
           'required for each BOT, CTD, LVS, and ROS cast'),
    'BO': 'Bottom time for cast. Usually taken for station position',
    'DE': 'Time mooring, float, drifter, XCP, XCTD, or XBT was deployed',
    'EN': 'Time cast completed',
    'MR': 'Time messenger was released on bottle or LVS cast',
    'RE': 'Time mooring, drifter, float, or other device recovered',
    'UN': 'Unknown',
}


navigation_system_codes = {
    'CIK': 'GLONASS - Russian version of GPS',
    'CN': 'Celestial navigation',
    'DEC': 'Decca',
    'DR': 'Dead reckoning (more accurate methods are preferred)',
    'GPS': 'Global Positioning System',
    'INS': 'Inertial navigation system',
    'LOR': 'Loran',
    'OM': 'Omega',
    'RDR': 'Radar fix',
    'TRS': 'Transit satellite system',
    'UNK': 'Unknown',
}


def identity_or_none(x):
    return x if x else None


def read(self, handle):
    '''How to read a Summary file for WOCE.'''
    header = True
    header_delimiter = re.compile('^-+$')
    column_starts = []
    column_widths = []
    for i, line in enumerate(handle):
        if header:
            if header_delimiter.match(line):
                header = False
                # Stops are tuples (beginning of column, end of column)
                # This is to delimit the columns of the sumfile
                stops = re.finditer('(\w+\s*)', self.globals['header'].split('\n')[-2])
                for stop in stops:
                    start = stop.start()
                    if len(column_starts) is 0:
                        column_starts.append(0)
                    else:
                        column_starts.append(start)
                    column_widths.append(stop.end()-start)
            else:
                self.globals['header'] += line
        else:
            if not line.strip():
                log.warn(u'Illegal empty line in summary file, row {0}'.format(i))
                continue
            tokens = []
            for s, w in zip(column_starts, column_widths):
                tokens.append(line[:-1][s:s+w].strip())
            if len(tokens) is 0:
                continue
            self['EXPOCODE'].append(tokens[0].replace('/', '_'))
            self['SECT_ID'].append(tokens[1])
            self['STNNBR'].append(tokens[2])
            self['CASTNO'].append(int_or_none(tokens[3]))
            self['_CAST_TYPE'].append(tokens[4])
            try:
                date = datetime.strptime(tokens[5], '%m%d%y')
                date = date.strftime('%Y%m%d')
            except ValueError, e:
                log.error(u'Expected date format %m%d%y. Got {0!r}.'.format(
                    tokens[5]))
                date = None
            self['DATE'].append(date)
            self['TIME'].append(int_or_none(tokens[6]))
            self['_CODE'].append(tokens[7])
            try:
                lat = woce.woce_lat_to_dec_lat(tokens[8].split())
            except ValueError:
                lat = woce.woce_dec_lat_to_dec_lat(tokens[8].split())
            except IndexError:
                lat = None
            self['LATITUDE'].append(lat)
            try:
                lng = woce.woce_lng_to_dec_lng(tokens[9].split())
            except ValueError:
                lng = woce.woce_dec_lng_to_dec_lng(tokens[9].split())
            except IndexError:
                lng = None
            self['LONGITUDE'].append(lng)
            try:
                self['_NAV'].append(tokens[10])
                self['DEPTH'].append(int_or_none(tokens[11]))
                self['_ABOVE_BOTTOM'].append(int_or_none(tokens[12]))
                self['_WIRE_OUT'].append(int_or_none(tokens[13]))
                self['_MAX_PRESSURE'].append(int_or_none(tokens[14]))
                self['_NUM_BOTTLES'].append(int_or_none(tokens[15]))
                self['_PARAMETERS'].append(identity_or_none(tokens[16]))
                self['_COMMENTS'].append(identity_or_none(tokens[17]))
            except IndexError:
                pass

    woce.fuse_datetime(self)
    self.check_and_replace_parameters()


def write(self, handle):
    '''How to write a Summary file for WOCE.'''
    woce.split_datetime(self)
    ship = self.globals.get('_SHIP', None) or '__SHIP__'
    leg = self.globals.get('_LEG', None) or '__LEG__'
    uniq_sects = uniquify(self['SECT_ID'].values)
    handle.write('%s LEG %s WHP-ID %s %s\n' % (ship, leg, ','.join(uniq_sects), config.stamp()))
    header_one = 'SHIP/CRS       WOCE               CAST         UTC           POSITION                UNC   COR ABOVE  WIRE   MAX  NO. OF\n'
    header_two = 'EXPOCODE       SECT STNNBR CASTNO TYPE DATE   TIME CODE LATITUDE   LONGITUDE   NAV DEPTH DEPTH BOTTOM  OUT PRESS BOTTLES PARAMETERS      COMMENTS            \n'
    header_sep = ('-' * (len(header_two) - 1)) + '\n'
    handle.write(header_one)
    handle.write(header_two)
    handle.write(header_sep)
    for i in range(0, len(self)):
        exdate = self.columns['DATE'][i]
        date_str = exdate[4:6] + exdate[6:8] + exdate[2:4]
        row = '%-14s %-5s %5s    ' % (
            self['EXPOCODE'][i], self['SECT_ID'][i], self['STNNBR'][i])
        row += '%3d  %3s %-6s %04s   ' % (
            self['CASTNO'][i], self['_CAST_TYPE'][i], date_str, self['TIME'][i])
        row += '%2s %-10s %-11s %3s %5d       ' % (
            self['_CODE'][i], woce.dec_lat_to_woce_lat(self['LATITUDE'][i]),
            woce.dec_lng_to_woce_lng(self['LONGITUDE'][i]), self['_NAV'][i],
            self['DEPTH'][i])
        row += '%-6d      ' % self['_ABOVE_BOTTOM'][i]
        row += '%5d %7d %-15s %-20s' % (
            self['_MAX_PRESSURE'][i], self['_NUM_BOTTLES'][i],
            self['_PARAMETERS'][i], self['_COMMENTS'][i])
        handle.write(row + '\n')
    woce.fuse_datetime(self)
