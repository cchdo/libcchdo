import datetime
import re
import struct


from .. import LOG
from .. import fns
from ..model import datafile
from ..fns import Decimal, _decimal, in_band_or_none


# Where no data is known
FILL_VALUE = -999.0


CHARACTER_PARAMETERS = ['STNNBR', 'SAMPNO', 'BTLNBR']


COLUMN_WIDTH = 8
SAFE_COLUMN_WIDTH = COLUMN_WIDTH - 1


END_DATA = 'END_DATA'


BOTTLE_FLAGS = {
    1: 'Bottle information unavailable.',
    2: 'No problems noted.',
    3: 'Leaking.',
    4: 'Did not trip correctly.',
    5: 'Not reported.',
    6: ('Significant discrepancy in measured values between Gerard and Niskin '
        'bottles.'),
    7: 'Unknown problem.',
    8: ('Pair did not trip correctly. Note that the Niskin bottle can trip at '
        'an unplanned depth while the Gerard trips correctly and vice versa.'),
    9: 'Samples not drawn from this bottle.',
}


CTD_FLAGS = {
    1: 'Not calibrated',
    2: 'Acceptable measurement',
    3: 'Questionable measurement',
    4: 'Bad measurement',
    5: 'Not reported',
    6: 'Interpolated over >2 dbar interval',
    7: 'Despiked',
    8: 'Not assigned for CTD data',
    9: 'Not sampled',
}


WATER_SAMPLE_FLAGS = {
    1: ('Sample for this measurement was drawn from water bottle but analysis '
        'not received.'),
    2: 'Acceptable measurement.',
    3: 'Questionable measurement.',
    4: 'Bad measurement.',
    5: 'Not reported.',
    6: 'Mean of replicate measurements.',
    7: 'Manual chromatographic peak measurement.',
    8: 'Irregular digital chromatographic peak integration.',
    9: 'Sample not drawn for this measurement from this bottle.',
}


def flag_description(flag_map):
    return ':'.join([':'] + ['%d = %s' % (i + 1, flag_map[i + 1]) for i in \
        range(len(flag_map))] + ["\n"])



BOTTLE_FLAG_DESCRIPTION = flag_description(BOTTLE_FLAGS)


CTD_FLAG_DESCRIPTION = flag_description(CTD_FLAGS)


WATER_SAMPLE_FLAG_DESCRIPTION = ':'.join([':'] + \
    ['%d = %s' % (i + 1, WATER_SAMPLE_FLAGS[i + 1]) for i in \
        range(len(WATER_SAMPLE_FLAGS))] + \
    ["\n"])


def woce_lat_to_dec_lat(lattoks):
    '''Convert a latitude in WOCE format to decimal.'''
    precision = 3 + len(lattoks)
    with fns.IncreasedPrecision(precision):
        lat = int(lattoks[0]) + Decimal(lattoks[1]) / Decimal('60.0')
        if lattoks[2] != 'N':
            lat *= -1
        return lat.quantize(Decimal(10) ** -precision)


def woce_lng_to_dec_lng(lngtoks):
    '''Convert a longitude in WOCE format to decimal.'''
    precision = 4 + len(lngtoks)
    with fns.IncreasedPrecision(precision):
        lng = int(lngtoks[0]) + Decimal(lngtoks[1]) / Decimal('60.0')
        if lngtoks[2] != 'E':
            lng *= -1
        return lng.quantize(Decimal(10) ** -precision)


def dec_lat_to_woce_lat(lat):
    '''Convert a decimal latitude to WOCE format.'''
    lat_deg = int(lat)
    lat_dec = abs(lat-lat_deg) * 60
    lat_deg = abs(lat_deg)
    lat_hem = 'S'
    if lat > 0:
        lat_hem = 'N'
    return '%2d %05.2f %1s' % (lat_deg, lat_dec, lat_hem)


def dec_lng_to_woce_lng(lng):
    '''Convert a decimal longitude to WOCE format.'''
    lng_deg = int(lng)
    lng_dec = abs(lng-lng_deg) * 60
    lng_deg = abs(lng_deg)
    lng_hem = 'W'
    if lng > 0 :
        lng_hem = 'E'
    return '%3d %05.2f %1s' % (lng_deg, lng_dec, lng_hem)


def strftime_woce_date_time(dt):
    if dt is None:
        return (None, None)
    if type(dt) is datetime.date:
    	return (dt.strftime('%Y%m%d'), None)
    return (dt.strftime('%Y%m%d'), dt.strftime('%H%M'))


def strptime_woce_date(woce_date):
    if woce_date is None:
        return None

    if '-' in str(woce_date):
        woce_date = str(woce_date).translate(None, '-')
    try:
        i_woce_date = int(woce_date)
        return datetime.datetime.strptime('%08d' % i_woce_date, '%Y%m%d').date()
    except (TypeError, ValueError):
        LOG.warn(u"Malformed date {0!r}. Omitting.".format(woce_date))
        return None


def strptime_woce_time(woce_time):
    try:
        i_woce_time = int(woce_time)
        if i_woce_time >= 2400:
            LOG.warn(
                u"Illegal time {0:04d} >= 2400. Setting to 0.".format(
                    i_woce_time))
            i_woce_time = 0
        return datetime.datetime.strptime('%04d' % i_woce_time, '%H%M').time()
    except (TypeError, ValueError):
        LOG.warn(u"Illegal time {0}. Setting to 0.".format(woce_time))
        return datetime.datetime.strptime('0000', '%H%M').time()


def strptime_woce_date_time(woce_date, woce_time):
    """ Parses WOCE date and time into a datetime or date object.
        Args:
            woce_date - a string representing a WOCE date YYYYMMDD
            woce_time - a string representing a WOCE time HHMM
        Returns:
            There are three non-trivial cases:
            1. DATE and TIME both exist
                datetime.datetime object representing the combination of the
                two objects.
            2. DATE exists and TIME does not
                datetime.date object representing the date.
            3. DATE does not exist but TIME does
                None
    """
    date = strptime_woce_date(woce_date)
    time = strptime_woce_time(woce_time)
    try:
        return datetime.datetime.combine(date, time)
    except TypeError:
        return date


def _bad_column_alignment(parameters):
    """ Determine by looking at any of the values if any column is mis-aligned.
        If there is a space anywhere inside a parameter value it is probably
        the result of a misalignment.
    """
    for i, p in enumerate(parameters):
        space_i = p.find(' ')
        if space_i > -1:
            return [i, (i - 1) * COLUMN_WIDTH + space_i]
    return [-1, -1]


def _remove_char_column(i, *lines):
    return [line[:i] + line[i + 1:] for line in lines]


def _remove_char_columns(cols, *lines):
    for col in cols:
        lines = _remove_char_column(col, *lines)
    return lines


def _warn_broke_character_column_rule(headername, headers):
    for header in headers:
        if len(header) > SAFE_COLUMN_WIDTH:
            LOG.warn("%s '%s' has too many characters (>%d)." % \
                     (headername, header, SAFE_COLUMN_WIDTH))


def _unpack_parameters(unpack_str, parameters_line, num_param_columns):
    parameters = fns.strip_all(
        struct.unpack(unpack_str, parameters_line[:num_param_columns * 8]))
    return (parameters, _bad_column_alignment(parameters))


def read_data(self, handle, parameters_line, units_line, asterisk_line):
    # num_quality_flags = the number of asterisk-marked columns
    num_quality_flags = len(re.findall('\*{7,8}', asterisk_line))
    num_quality_words = len(parameters_line.split('QUALT'))-1

    LOG.debug(u'{0} quality flags, {1} quality words'.format(
        num_quality_flags, num_quality_words))

    # The extra 1 in quality_length is for spacing between the columns
    quality_length = num_quality_words * (max(len('QUALT#'),
                                              num_quality_flags) + 1)
    num_param_columns = int(
        (len(parameters_line) - quality_length) / COLUMN_WIDTH)

    # Unpack the column headers
    unpack_str = '8s' * num_param_columns

    bad_cols = []

    parameters, start_bad = _unpack_parameters(
        unpack_str, parameters_line, num_param_columns)
    while start_bad[0] > -1:
        bad_cols.append(start_bad[1])
        LOG.error('Bad column alignment starting at character %d' % \
                  start_bad[1])
        # Attempt recovery by removing that column.
        parameters_line, units_line, asterisk_line = \
            _remove_char_column(start_bad[1], parameters_line,
                                units_line, asterisk_line)
        parameters, start_bad = _unpack_parameters(
            unpack_str, parameters_line, num_param_columns)

    units = fns.strip_all(
        struct.unpack(unpack_str, units_line[:num_param_columns * 8]))
    asterisks = fns.strip_all(
        struct.unpack(unpack_str, asterisk_line[:num_param_columns * 8]))

    # Warn if the header lines break 8 character column rules
    _warn_broke_character_column_rule("Parameter", parameters)
    _warn_broke_character_column_rule("Unit", units)
    _warn_broke_character_column_rule("Asterisks", asterisks)

    # Die if parameters are not unique
    if not parameters == fns.uniquify(parameters):
        raise ValueError(('There were duplicate parameters in the file; '
                          'cannot continue without data corruption.'))

    self.create_columns(parameters, units)

    # Get each data line
    unpack_data_str = unpack_str

    # Add on quality to unpack string
    quality_word_spacing = \
        quality_length / num_quality_words - num_quality_flags
    unpack_str += ('%sx%ss' % (quality_word_spacing, num_quality_flags)) * \
                  num_quality_words

    # Let's be nice and try to handle the case where there's a little extra
    # space before the quality flags but everything else is fairly well-formed.
    # It is possible for there to be lots of space between flags and data, so
    # try a few times.
    original_unpack_str = unpack_str
    savepoint = handle.tell()
    line = handle.readline().rstrip()
    determined_num_columns = False
    tries = 0
    while tries < 5:
        try:
            unpacked = struct.unpack(unpack_str, line)
            determined_num_columns = True
            break
        except struct.error, e:
            expected_len = struct.calcsize(unpack_str)
            LOG.warn(
                'Data record 0 has length %d (expected %d).' % (
                    len(line), expected_len))
            LOG.info('There is likely extra columns of space between data '
                     'and flags. Detecting whether this is the case.')
            quality_word_spacing += 1
            tries += 1
            unpack_str = unpack_data_str + \
                ('%sx%ss' % (quality_word_spacing, num_quality_flags)
                ) * num_quality_words
    if not determined_num_columns:
        unpack_str = original_unpack_str
    handle.seek(savepoint)
    LOG.debug(u'Settled on unpack format: {0!r}'.format(unpack_str))

    for i, line in enumerate(handle):
        line = _remove_char_columns(bad_cols, line.rstrip())[0]
        if not line:
            raise ValueError('Empty lines are not allowed in the data section '
                             'of a WOCE file')
        try:
            unpacked = struct.unpack(unpack_str, line)
        except struct.error, e:
            expected_len = struct.calcsize(unpack_str)
            LOG.warn('Data record %d has length %d (expected %d).' % (
                i, len(line), expected_len))
            raise e

        # QUALT1 takes precedence
        quality_flags = unpacked[-num_quality_words:]

        # Build up the columns for the line
        flag_i = 0
        for j, parameter in enumerate(parameters):
            datum = unpacked[j].strip()
            datum = in_band_or_none(datum, -9)

            if parameter not in CHARACTER_PARAMETERS:
                try:
                    datum = _decimal(datum)
                except Exception, e:
                    LOG.warning(
                        u'Expected numeric data for parameter %r, got %r' % (
                        parameter, datum))

            # Only assign flag if column is flagged.
            if "**" in asterisks[j].strip():
                # TODO should use better detection for asterisks
                try:
                    woce_flag = int(quality_flags[0][flag_i])
                except ValueError, e:
                    LOG.error(
                        u'Received bad flag "{}" for {} on record {}'.format(
                        quality_flags[0][flag_i], parameter, i))
                    raise e
                flag_i += 1
                self.columns[parameter].set(i, datum, woce_flag)
            else:
                self.columns[parameter].set(i, datum)

    # Expand globals into columns TODO?


def write_data(self, handle, ):
    def parameter_name_of (column, ):
        return column.parameter.mnemonic_woce()

    def units_of (column, ):
        if column.parameter.units:
            return column.parameter.units.mnemonic
        else:
            return ''

    def quality_flags_of (column, ):
        return "*******" if column.is_flagged_woce() else ""

    def countable_flag_for (column, ):
        return 1 if column.is_flagged_woce() else 0

    num_qualt = sum(map(
            countable_flag_for, self.columns.values() ))

    base_format = "%8s" * len(self.columns)
    qualt_colsize = max( (len(" QUALT#"), num_qualt) )
    qualt_format = "%%%ds" % qualt_colsize
    base_format += qualt_format
    base_format += "\n"

    columns = self.sorted_columns()

    all_headers = map(parameter_name_of, columns)
    all_units = map(units_of, columns)
    all_asters = map(quality_flags_of, columns)

    all_headers.append(qualt_format % "QUALT1")
    all_units.append(qualt_format % "")
    all_asters.append(qualt_format % "")

    handle.write(base_format % tuple(all_headers))
    handle.write(base_format % tuple(all_units))
    handle.write(base_format % tuple(all_asters))

    nobs = max(map(len, columns))
    for i in range(nobs):
        values = []
        flags = []
        for column in columns:
            format = column.parameter.format
            if column[i]:
                values.append(format % column[i])
            if column.is_flagged_woce():
                flags.append(str(column.flags_woce[i]))

        values.append("".join(flags))
        handle.write(base_format % tuple(values))


def fuse_datetime(file):
    """ Fuses a file's "DATE" and "TIME" columns into a "_DATETIME" column.
        There are three cases:
        1. DATE and TIME both exist
            A datetime.datetime object is inserted representing the combination
            of the two objects.
        2. DATE exists and TIME does not
            A datetime.date object is inserted only representing the date.
        3. DATE does not exist but TIME does
            None is inserted because date is required.

        Arg:
            file - a DataFile object
    """
    try:
        dates = file['DATE'].values
    except KeyError:
        LOG.error(u'No DATE column is present.')
        return

    try:
        times = file['TIME'].values
    except KeyError:
        LOG.warn(u'No TIME column is present.')

    file['_DATETIME'] = datafile.Column('_DATETIME')
    file['_DATETIME'].values = [strptime_woce_date_time(*x) for x in zip(
        dates, times)]
    del file['DATE']
    del file['TIME']


def split_datetime(file):
    """ Splits a file's "_DATETIME" columns into "DATE" and "TIME" columns.

        There are three cases:
        1. datetime
            DATE and TIME are populated appropriately.
        2. date
            Only DATE is populated.
        3. None
            Both DATE and TIME are None

        If there are absolutely no TIMEs in the file the TIME column is not
        kept.

        Arg:
            file - a DataFile object
    """
    date = file['DATE'] = datafile.Column('DATE')
    time = file['TIME'] = datafile.Column('TIME')
    for dtime in file['_DATETIME'].values:
        if dtime:
            date.append(dtime.strftime('%Y%m%d'))
            if type(dtime) is datetime.datetime:
                time.append(dtime.strftime('%H%M'))
        else:
            date.append(None)
            time.append(None)
    del file['_DATETIME']

    if not any(file['TIME'].values):
    	del file['TIME']


_MSG_NO_STN_CAST_PAIR = (u'The station cast pair ({}, {}) was not found in '
                         'the summary file.')


def combine(woce_file, sum_file):
    """Combines the given WOCE file with the Summary WOCE file.

    This entails merging the datetime, location, and depth data based on station
    and cast.

    The resulting DataFile contains most of the information from both files.

    """
    headers = sum_file.get_property_for_columns(lambda c: c.parameter.name)

    if woce_file.globals.get('STNNBR', None) is not None:
        # This is probably a CTD file.
        station = woce_file.globals.get('STNNBR')
        cast = woce_file.globals.get('CASTNO')
        try:
            sum_file_index = sum_file.index(station, cast)
        except ValueError, e:
            LOG.error(_MSG_NO_STN_CAST_PAIR.format(station, cast))
            raise e
        values = sum_file.get_property_for_columns(lambda c: c[sum_file_index])

        info = dict(zip(headers, values))
        woce_file.globals['EXPOCODE'] = info['EXPOCODE']
        woce_file.globals['SECT_ID'] = info['SECT_ID']
        woce_file.globals['_DATETIME'] = info['_DATETIME']
        woce_file.globals['LATITUDE'] = info['LATITUDE']
        woce_file.globals['LONGITUDE'] = info['LONGITUDE']
        woce_file.globals['DEPTH'] = info['DEPTH']
    else:
        # This is probably a Bottle file.
        station_col = woce_file['STNNBR']
        cast_col = woce_file['CASTNO']

        woce_file.ensure_column('EXPOCODE')
        woce_file.ensure_column('SECT_ID')
        woce_file.ensure_column('_DATETIME')
        woce_file.ensure_column('LATITUDE')
        woce_file.ensure_column('LONGITUDE')
        woce_file.ensure_column('DEPTH')

        for i in range(len(woce_file)):
            station = station_col[i]
            cast = cast_col[i]
            try:
                sum_file_index = sum_file.index(station, cast)
            except ValueError, e:
                LOG.error(_MSG_NO_STN_CAST_PAIR.format(station, cast))
                raise e
            values = sum_file.get_property_for_columns(
                lambda c: c[sum_file_index])

            info = dict(zip(headers, values))
            woce_file['EXPOCODE'][i] = info['EXPOCODE']
            woce_file['SECT_ID'][i] = info['SECT_ID']
            woce_file['_DATETIME'][i] = info['_DATETIME']
            woce_file['LATITUDE'][i] = info['LATITUDE']
            woce_file['LONGITUDE'][i] = info['LONGITUDE']
            woce_file['DEPTH'][i] = info['DEPTH']
        woce_file.globals['header'] = ''
