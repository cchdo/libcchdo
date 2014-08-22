# -*- coding: utf-8 -*-
"""LDEO ASEP format

Some samples:

http://www.ldeo.columbia.edu/~claudiag/ASEP/readme_ASEP_data_march2011.html
ftp://ftp.nodc.noaa.gov/nodc/archive/arc0018/0036202/1.1/about/as_format.txt
ftp://ftp.nodc.noaa.gov/nodc/archive/arc0061/0112105/1.1/data/0-data/lb01_format.txt


"""
from datetime import datetime
from logging import getLogger


log = getLogger(__name__)


from libcchdo.algorithms.depth import depth_unesco
from libcchdo.db.model.std import Parameter, Unit
from libcchdo.fns import _decimal
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = []


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


def _getline(fileobj):
    return fileobj.readline().rstrip()


CTD_PARAM_MAP = {
# pr  Pressure [decibars].
    'pr': Parameter('CTDPRS', units=Unit('DBAR')),
# te  In-situ temperature [°C] (IPTS-68).
    'te': Parameter('CTDTMP', units=Unit('IPTS-68')),
# sa  Salinity (PSS-78).
    'sa': Parameter('CTDSAL', units=Unit('PSS-78')),
# ox  Oxygen [ml/l].
    'ox': Parameter('CTDOXY', units=Unit('ML/L')),
# de       Depth [m].
    'de': Parameter('DEPTH', units=Unit('METERS')),
# tr  Light transmission [%].
    'tr': Parameter('XMISS', units=Unit('%')),
# pt  Potential Temperature [°C].
    'pt': None,
}


def is_datatype_ctd(dtype):
    return dtype == 'C'


def pad_decimal(dec, decplaces):
    needed = decplaces + dec.as_tuple().exponent
    strrep = str(dec)
    if '.' not in strrep:
        strrep += '.'
    return _decimal(strrep + '0' * needed)


def read(self, fileobj):
    """How to read an LDEO ASEP file."""
    line1 = _getline(fileobj)

    dtype_shipcode, stn, cast, lat, lon, date, yday, time, cruise_id = \
        line1.split()

    dtype = dtype_shipcode[0]

    if not is_datatype_ctd(dtype):
        log.error(u'Unable to read non-CTD ASEP files at the moment.')
        return

    shipcode = dtype_shipcode[1:]
    # FIXME this is not really the EXPOCODE
    self.globals['EXPOCODE'] = cruise_id
    # FIXME this is not really the SECT_ID
    self.globals['SECT_ID'] = cruise_id
    self.globals['STNNBR'] = str(int(stn))
    self.globals['CASTNO'] = cast
    self.globals['LATITUDE'] = lat
    self.globals['LONGITUDE'] = lon
    self.globals['_DATETIME'] = datetime.strptime(date + time, '%Y/%m/%d%H:%M')
    self.globals['header'] = '#' + cruise_id

    line2 = _getline(fileobj)
    while line2[0] != '&':
        log.warn(u'Ignoring line not preceded by &: {0!r}'.format(line2))
        line2 = _getline(fileobj)

    self.globals['header'] += "\n#" + line2 + "\n"

    line3 = _getline(fileobj)
    while line3[0] != '@':
        log.warn(u'Ignoring line not preceded by @: {0!r}'.format(line2))
        line3 = _getline(fileobj)

    param_keys = line3[1:].split()
    parameters = [CTD_PARAM_MAP.get(key, None) for key in param_keys]
    cols = self.create_columns(parameters)
    for line in fileobj:
        for col, val in zip(cols, line.split()):
            if val == '-9':
                val = None
            col.append(_decimal(val))

    # rewrite every data column to be the same sigfigs
    for col in self.columns.values():
        decplaces = col.decimal_places()
        col.values = [pad_decimal(val, decplaces) for val in col.values]

    if 'pr' in param_keys:
        pressures = cols[param_keys.index('pr')].values
        lat = _decimal(self.globals['LATITUDE'])
        depth = int(depth_unesco(pressures[-1], lat))
        self.globals['DEPTH'] = depth

    self.check_and_replace_parameters()
