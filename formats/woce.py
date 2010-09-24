'''libcchdo.formats.woce'''


import datetime
import re
import struct


import libcchdo
import libcchdo.model.datafile


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


BOTTLE_FLAG_DESCRIPTION = ':'.join([':'] + \
    ['%d = %s' % (i + 1, BOTTLE_FLAGS[i + 1]) for i in \
        range(len(BOTTLE_FLAGS))] + \
    ["\n"])


WATER_SAMPLE_FLAG_DESCRIPTION = ':'.join([':'] + \
    ['%d = %s' % (i + 1, WATER_SAMPLE_FLAGS[i + 1]) for i in \
        range(len(WATER_SAMPLE_FLAGS))] + \
    ["\n"])


def woce_lat_to_dec_lat(lattoks):
    '''Convert a latitude in WOCE format to decimal.'''
    lat = int(lattoks[0]) + float(lattoks[1]) / 60.0
    if lattoks[2] != 'N':
        lat *= -1
    return lat


def woce_lng_to_dec_lng(lngtoks):
    '''Convert a longitude in WOCE format to decimal.'''
    lng = int(lngtoks[0]) + float(lngtoks[1]) / 60.0
    if lngtoks[2] != 'E':
        lng *= -1
    return lng


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


def strftime_woce_date_time(dtime):
    if dtime is None:
        return (None, None)
    return (dtime.strftime('%Y%m%d'), dtime.strftime('%H%M'))


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
    try:
        i_woce_date = int(woce_date)
        d = datetime.datetime.strptime('%08d' % i_woce_date, '%Y%m%d').date()
    except TypeError:
        return None
    except ValueError:
        return None
    
    try:
        i_woce_time = int(woce_time)
        if i_woce_time >= 2400:
            libcchdo.LOG.warn("Illegal time > 2400. Setting to 0.")
            i_woce_time = 0
        t = datetime.datetime.strptime('%04d' % i_woce_time, '%H%M').time()
    except TypeError:
        return d
    except ValueError:
        return d

    return datetime.datetime.combine(d, t)


def read_data(self, handle, parameters_line, units_line, asterisk_line):
    column_width = 8
    safe_column_width = column_width - 1

    # num_quality_flags = the number of asterisk-marked columns
    num_quality_flags = len(re.findall('\*{7,8}', asterisk_line))
    num_quality_words = len(parameters_line.split('QUALT'))-1

    # The extra 1 in quality_length is for spacing between the columns
    quality_length = num_quality_words * (max(len('QUALT#'),
                                              num_quality_flags) + 1)
    num_param_columns = int((len(parameters_line) - quality_length) / \
                             column_width)

    # Unpack the column headers
    unpack_str = '8s' * num_param_columns
    parameters = libcchdo.fns.strip_all(struct.unpack(unpack_str,
                                  parameters_line[:num_param_columns*8]))
    units = libcchdo.fns.strip_all(struct.unpack(unpack_str,
                                    units_line[:num_param_columns*8]))
    asterisks = libcchdo.fns.strip_all(struct.unpack(unpack_str,
                                 asterisk_line[:num_param_columns*8]))

    # Warn if the header lines break 8 character column rules
    def warn_broke_character_column_rule(headername, headers):
        for header in headers:
            if len(header) > safe_column_width:
                libcchdo.LOG.warn("%s '%s' has too many characters (>%d)." % \
                                  (headername, header, safe_column_width))

    warn_broke_character_column_rule("Parameter", parameters)
    warn_broke_character_column_rule("Unit", units)
    warn_broke_character_column_rule("Asterisks", asterisks)

    # Die if parameters are not unique
    if not parameters == libcchdo.fns.uniquify(parameters):
        raise ValueError(('There were duplicate parameters in the file; '
                          'cannot continue without data corruption.'))

    self.create_columns(parameters, units)

    # Get each data line
    # Add on quality to unpack string
    unpack_str += ('%sx%ss' % (quality_length / num_quality_words - \
                              num_quality_flags, num_quality_flags)) * \
                  num_quality_words
    for i, line in enumerate(handle):
        unpacked = struct.unpack(unpack_str, line.rstrip())

        # QUALT1 takes precedence
        quality_flags = unpacked[-num_quality_words:]

        # Build up the columns for the line
        flag_i = 0
        for j, parameter in enumerate(parameters):
            datum = float(unpacked[j])
            if datum is -9.0:
                datum = None
            woce_flag = None

            # Only assign flag if column is flagged.
            if "**" in asterisks[j].strip(): # XXX
                woce_flag = int(quality_flags[0][flag_i])
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
    datecol = file['DATE']
    timecol = file['TIME']
    file['_DATETIME'] = libcchdo.model.datafile.Column('_DATETIME')
    file['_DATETIME'].values = [strptime_woce_date_time(*x) for x in zip(
            datecol.values, timecol.values)]
    del datecol
    del timecol


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
    date = file['DATE'] = libcchdo.model.datafile.Column('DATE')
    time = file['TIME'] = libcchdo.model.datafile.Column('TIME')
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


