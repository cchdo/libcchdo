'''libcchdo.formats.ctd.bacp'''

import re
import datetime

import libcchdo

def read(self, handle):
    '''How to read a CTD BACp file.'''
    if not handle.readline().startswith('CLIVAR BACp data'):
        raise ValueError('This file is not a BACp file.')

    sect_id = ' '.join(handle.readline().strip().split()[1:])
    date_tuple = map(int, handle.readline().strip()[len('Date:'):].split())
    time_tuple = map(int, handle.readline().strip()[len('Time '):].split())
    lat = handle.readline().split(':')[1].strip()
    lng = handle.readline().split(':')[1].strip()
    depth = handle.readline().split(':')[1].strip()
    station = handle.readline().split(':')[1].strip()
    cast = handle.readline().split(':')[1].strip()
    dtime = datetime.datetime(*(date_tuple + time_tuple))

    columns = ('CTDPRS', 'XMISS', )
    units = ('DBAR', '', )

    self.create_columns(columns, units)

    for l in handle:
        for i, v in enumerate(map(float, l.split())):
            self.columns[columns[i]].append(v)

    self.check_and_replace_parameters()
