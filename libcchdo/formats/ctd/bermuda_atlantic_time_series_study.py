import datetime
from decimal import Decimal

from ... import LOG
from .. import woce


def _timedelta_to_seconds(td):
    return td.days * 3600 * 24 + td.seconds + td.microseconds * 1e-9


def _idparts(filename):
    try:
        idpart = filename[:filename.rindex('.dpr')]
    except ValueError:
        LOG.warn('BATS filename does not end in .dpr')
        return (None, None, None)
    type_id = idpart[0]
    cruise_id = idpart[1:5]
    cast_id = idpart[6]
    return (type_id, cruise_id, cast_id)


def is_global(column):
    check = None
    for x in column:
        if check is None:
            check = x
            continue
        if check != x:
            return False
    return True


def _append_to_column(column, value):
    flag = 2
    if not column.parameter.is_in_range(value):
        flag = 9
        value = None
    column.append(value, flag)


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

        start = datetime.datetime(year, 1, 1)
        seconds_in_year = Decimal(
            str(_timedelta_to_seconds(
                    datetime.datetime(year + 1, 1, 1) - start))) * \
            Decimal('0.' + frac_year)
        dt = datetime.datetime(1970, 1, 1) + \
            datetime.timedelta(
                seconds=float(int(start.strftime('%s')) + seconds_in_year))

        self['_DATETIME'].append(dt)
        self['LATITUDE'].append(Decimal(parts[2]))
        # BATS files don't record negative longitude because their study area
        # is small.
        self['LONGITUDE'].append(Decimal('-' + parts[3]))

        _append_to_column(self['CTDPRS'], Decimal(parts[4]))
        _append_to_column(self['CTDTMP'], Decimal(parts[6]))
        _append_to_column(self['CTDSAL'], Decimal(parts[7]))
        _append_to_column(self['CTDOXY'], Decimal(parts[8]))
        _append_to_column(self['FLUOR'], Decimal(parts[10]))

    self.globals['_COMMENTS'] = ';'.join(comments)
    self.globals['DEPTH'] = -999
    self.globals['SECT_ID'] = 'ARS20'

    type_id, cruise_id, cast_id = _idparts(handle.name)

    self.globals['STNNBR'] = cruise_id
    self.globals['CASTNO'] = cast_id

    if is_global(self['_DATETIME']):
        date_str, time_str = woce.strftime_woce_date_time(self['_DATETIME'][0])
        self.globals['DATE'] = date_str
        self.globals['TIME'] = time_str
        del self['_DATETIME']

    self.globals['EXPOCODE'] = '33H4%s' % date_str

    if is_global(self['LATITUDE']):
        self.globals['LATITUDE'] = self['LATITUDE'][0]
        del self['LATITUDE']

    if is_global(self['LONGITUDE']):
        self.globals['LONGITUDE'] = self['LONGITUDE'][0]
        del self['LONGITUDE']
