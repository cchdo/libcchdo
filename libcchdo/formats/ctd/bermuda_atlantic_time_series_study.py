from logging import getLogger


log = getLogger(__name__)


from libcchdo.fns import _decimal, equal_with_epsilon
from libcchdo.formats.exchange import FILL_VALUE
from libcchdo.datadir import create_expocode
from libcchdo.formats.bermuda_atlantic_time_series_study import (
    dpr_idparts, bats_time_to_dt, BATS_SECT_ID, correct_longitude,
    collapse_globals,
    )


def _decimal_check_missing(str):
    """Convert str to a decimal or None if matches dpr fill value."""
    x = _decimal(str)
    if equal_with_epsilon(x, -9.99) or equal_with_epsilon(x, -10):
        return None
    return x


def read(self, handle):
    """How to read a CTD Bermuda Atlantic Time-Series Study file."""
    comments = []

    columns = ('_DATETIME', 'LATITUDE', 'LONGITUDE', 'CTDPRS', 'CTDTMP',
               'CTDSAL', 'CTDOXY', 'FLUOR', )
    units = ('', '', '', 'DBAR', 'DEG C', 'PSU', 'UMOL/KG', 'RFU', )

    self.create_columns(columns, units)
    self.check_and_replace_parameters(convert=False)

    for l in handle:
        if l.startswith('%'):
            comments.append(l[1:].strip())
            continue
        parts = l.split()
        year, frac_year = parts[1].split('.')
        year = int(year)

        self['_DATETIME'].append(bats_time_to_dt(parts[1]))
        self['LATITUDE'].append(_decimal(parts[2]))
        self['LONGITUDE'].append(_decimal(correct_longitude(parts[3])))

        self['CTDPRS'].append_check_range(_decimal_check_missing(parts[4]))
        self['CTDTMP'].append_check_range(_decimal_check_missing(parts[6]))
        self['CTDSAL'].append_check_range(_decimal_check_missing(parts[7]))
        self['CTDOXY'].append_check_range(_decimal_check_missing(parts[8]))
        self['FLUOR'].append_check_range(_decimal_check_missing(parts[10]))

    self.globals['_COMMENTS'] = ';'.join(comments)
    self.globals['EXPOCODE'] = create_expocode('33H4', self['_DATETIME'][0])
    self.globals['SECT_ID'] = BATS_SECT_ID
    idparts = dpr_idparts(handle.name)
    self.globals['_OS_ID'] = idparts['cruise']
    self.globals['STNNBR'] = idparts['type']
    self.globals['CASTNO'] = idparts['cast']
    self.globals['DEPTH'] = FILL_VALUE

    collapse_globals(self, ['_DATETIME', 'LATITUDE', 'LONGITUDE'])
