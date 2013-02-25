import os.path
from datetime import datetime, timedelta

from libcchdo import LOG
from libcchdo.fns import Decimal, equal_with_epsilon


BATS_SECT_ID = 'ARS20'


def dpr_idparts(filename):
    """Extract type, cruise, and cast ids from a BATS .dpr (CTD) file."""
    try:
        filename = os.path.basename(filename)
        idpart, ext = os.path.splitext(filename)
    except ValueError:
        LOG.warn('BATS CTD filename {0!r} should end in .dpr'.format(filename))
        return {'type': None, 'cruise': None, 'cast': None}
    type_id = idpart[0]
    cruise_id = idpart[1:5]
    cast_id = idpart[6]
    return {'type': type_id, 'cruise': cruise_id, 'cast': cast_id}


def _timedelta_to_seconds(td):
    return td.total_seconds() + td.microseconds * 1e-9


def bats_time_to_dt(bats_time):
    dyear = float(bats_time)
    year = int(dyear)

    start = datetime(year, 1, 1)
    td_year = datetime(year + 1, 1, 1) - start
    seconds_in_year = _timedelta_to_seconds(td_year)
    seconds_of_year = seconds_in_year * (dyear - year)
    return start + timedelta(seconds=seconds_of_year)


BATS_SECT_ID = 'ARS20'


def dpr_idparts(filename):
    """Extract type, cruise, and cast ids from a BATS .dpr (CTD) file."""
    try:
        filename = os.path.basename(filename)
        idpart, ext = os.path.splitext(filename)
    except ValueError:
        LOG.warn('BATS CTD filename {0!r} should end in .dpr'.format(filename))
        return {'type': None, 'cruise': None, 'cast': None}
    type_id = idpart[0]
    cruise_id = idpart[1:5]
    cast_id = idpart[6]
    return {'type': type_id, 'cruise': cruise_id, 'cast': cast_id}


def _timedelta_to_seconds(td):
    return td.total_seconds() + td.microseconds * 1e-9


def bats_time_to_dt(bats_time):
    dyear = float(bats_time)
    year = int(dyear)

    start = datetime(year, 1, 1)
    td_year = datetime(year + 1, 1, 1) - start
    seconds_in_year = _timedelta_to_seconds(td_year)
    seconds_of_year = seconds_in_year * (dyear - year)
    return start + timedelta(seconds=seconds_of_year)


def deg_min_to_decimal_deg(deg, min):
    return Decimal(deg) + Decimal(min) / Decimal(60)


def correct_longitude(str):
    """Correct BATS file longitude.

    BATS doesn't record their longitudes with signs because their study area is
    small. Bermuda is near -60 so all longitudes should be negative.

    """
    return '-' + str


def collapse_globals(df, parameters):
    """Check each specified column for globality and set the global.

    Check whether the column values are all the same. If they are, set the
    file's global key value and delete the column.

    Arguments:
    parameters - a list of string parameter names

    """
    for p in parameters:
        try:
            if df[p] and df[p].is_global():
                df.globals[p] = df[p][0]
                del df[p]
            else:
                LOG.info(
                    u'Cannot collapse {0} to global. More than one unique '
                    'value exists: {1}'.format(p, df[p].values))
        except KeyError:
            LOG.debug(u'No such key to check globality: {0}'.format(p))


def combine(bats_file, event_sum_file):
    """Combines the given BATS .dpr file with the Summary event.log file so
       that the DataFile contains most of the information from both.
    """
    # It is pretty much given that the data is CTD.

    lat, lng = bats_file.globals['LATITUDE'], bats_file.globals['LONGITUDE']

    # Find the event log record
    sum_file_i = None
    for i in range(len(event_sum_file)):
        sumlat, sumlng = event_sum_file['LATITUDE'][i], event_sum_file['LONGITUDE'][i]
        epsilon = Decimal('1e-3')
        close_enough = equal_with_epsilon(lat, sumlat, epsilon) and \
                       equal_with_epsilon(lng, sumlng, epsilon)
        if close_enough:
            sum_file_i = i
            break

    if sum_file_i is None:
        LOG.error('Event for BATS data at %f %f not found' % (lat, lng))
        return
    headers = event_sum_file.column_headers()
    row = event_sum_file.row(i)

    info = dict(zip(headers, row))
    bats_file.globals['DEPTH'] = info['DEPTH']
