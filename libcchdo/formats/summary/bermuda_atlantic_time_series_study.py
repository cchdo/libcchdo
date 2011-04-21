import re
import datetime
import os

from .. import bermuda_atlantic_time_series_study as bats


def read(self, handle):
    """How to read a quasi-Summary 'event.log' file for BATS."""
    comments = []
    for line in handle:
        if line.startswith('%'):
            comments.append(line[1:].lstrip())
            continue
        tokens = line.split()
        if len(tokens) is 0:
            continue

        # 1 is the BATS event code for CTD
        if tokens[2] != '1':
            continue

        # Prefer 'in' attributes because there must be at least an in.
        if tokens[4] != '1':
            continue

        # Short for BATS UNKNOWN
        stnnbr = 'BATSUNK'
        if os.path.basename(handle.name).startswith('1'):
            # Short for BATS CORE
            stnnbr = 'BATSCR'
        elif os.path.basename(handle.name).startswith('6'):
            stnnbr = 'HYDROS'

        self['STNNBR'].append(stnnbr)
        self['CASTNO'].append(int(tokens[3]))
        self['_CAST_TYPE'].append('CTD')

        self['DATE'].append(datetime.datetime.strptime(tokens[0], '%Y%m%d').date())
        self['TIME'].append(datetime.datetime.strptime('%04d' % int(tokens[1]), '%H%M').time())

        self['_CODE'].append('BE')

        lat = bats.deg_min_to_decimal_deg(*tokens[6:8])
        self['LATITUDE'].append(lat)
        lng = -bats.deg_min_to_decimal_deg(*tokens[8:10])
        self['LONGITUDE'].append(lng)

        self['_NAV'].append(None)
        self['DEPTH'].append(int(tokens[5]))
        self['_ABOVE_BOTTOM'].append(None)
        self['_MAX_PRESSURE'].append(None)
        self['_NUM_BOTTLES'].append(None)
        self['_PARAMETERS'].append(None)
        self['_COMMENTS'].append(None)

    self.check_and_replace_parameters(convert=False)


# OMIT write
