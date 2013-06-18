import datetime
import re

from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = ['coriolis']


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


def read(self, handle):
    """How to read a Coriolis file."""
    expocode, sect, sectid, ship, program = handle.readline()[1:].split()
    dates = handle.readline()
    unknown = handle.readline()
    pi = handle.readline()
    for i in range(7):
        handle.readline()

    self.create_columns(
        ('EXPOCODE', 'SECT_ID', '_DATETIME', 'LATITUDE', 'LONGITUDE'))

    # PRES -> CTDPRS CCHDO has no other measurement of pressure
    # TEMP -> CTDTMP Probably not reference nor potential
    # PSAL -> SALNTY Probably a bottle (PSU doesn't match CCHDO's PSS-78)
    # CPHL -> CHLORA (mg/m**3 doesn't match CCHDO's ug/kg)
    # PHOS -> PHSPHT (umol/l doesn't match CCHDO's umol/kg)
    # NTRZ -> NO2+NO3 (umol/l doesn't match CCHDO's umol/kg)
    self.create_columns(
        ('CTDPRS', 'CTDTMP', 'SALNTY', 'CHLORA', 'PHSPHT', 'NO2+NO3'),
        ('DBAR', 'DEG C', 'PSU', 'MG/M3', 'UMOL/L', 'UMOL/L'))

    while handle:
        if not handle.readline():
            break
        navitems = handle.readline()[1:].split()
        date = navitems[0].split('=')[1]
        time = navitems[1].split('=')[1]
        lat = navitems[2].split('=')[1] + ' ' + navitems[3]
        lng = navitems[4].split('=')[1] + ' ' + navitems[5]
        qcnav = navitems[7].split('=')[1]

        dtime = datetime.datetime.strptime(date + time, '%d%m%Y%H%M')
        lats = lat.split()
        latitude = int(lats[0][1:])
        if lats[0].startswith('S'):
            latitude *= -1
        latitude += float(lats[1]) / 60.0

        lngs = lng.split()
        longitude = int(lngs[0][1:])
        if lngs[0].startswith('W'):
            longitude *= -1
        longitude += float(lngs[1]) / 60

        for i in range(17):
            handle.readline()

        line = handle.readline()
        while not line.strip().endswith('999'):
            data = line.strip().split()
            flags = map(int, data[-1])
            data = map(float, data[:-1])

            self.columns['EXPOCODE'].append(expocode)
            self.columns['SECT_ID'].append(sect + sectid)
            self.columns['_DATETIME'].append(dtime)
            self.columns['LATITUDE'].append(latitude)
            self.columns['LONGITUDE'].append(longitude)
            self.columns['CTDPRS'].append(data[0], flags[0])
            self.columns['CTDTMP'].append(data[1], flags[1])
            self.columns['SALNTY'].append(data[2], flags[2])
            self.columns['CHLORA'].append(data[3], flags[3])
            self.columns['PHSPHT'].append(data[4], flags[4])
            self.columns['NO2+NO3'].append(data[5], flags[5])

            line = handle.readline()

    self.globals['stamp'] = ''
    self.globals['header'] = ''

    self.check_and_replace_parameters()
