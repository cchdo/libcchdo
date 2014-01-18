"""Exchange related functions.

"""
from re import compile as re_compile, match as re_match

from libcchdo.config import stamp as user_stamp
from libcchdo.log import LOG
from libcchdo.fns import Decimal, decimal_to_str, _decimal, out_of_band
from libcchdo.db.model.std import session
from libcchdo.db.model.convert import find_parameter
from libcchdo.formats.stamped import read_stamp


# Where no data is known
FILL_VALUE = -999.0


FLAG_ENDING_WOCE = '_FLAG_W'
FLAG_ENDING_IGOSS = '_FLAG_I'


END_DATA = 'END_DATA'


r_idstamp = re_compile('(\w+)')
r_stamp = re_compile('\d{8}\w+')


def read_type_and_stamp(fileobj):
    """Only get the file type and stamp line.

    For zipfiles, return the most common stamp and warn if there is more than
    one.

    """
    def reader(fobj):
        return fobj.readline().rstrip().split(',', 1)
    return read_stamp(fileobj, reader)


def read_identifier_line(dfile, fileobj, ftype):
    """Read an Exchange identifier line from the fileobj to the dfile.

    An Exchange identifier line begins with either "BOTTLE," or "CTD," and ends
    with a WOCE style stamp.

    Raises:
        ValueError - if either the file type is not one of BOTTLE or CTD or the
            identifier line is malformed.

    """
    read_ftype, stamp = read_type_and_stamp(fileobj)
    if ftype != read_ftype:
        raise ValueError(
            u'Expected Exchange file type {0!r} and got {1!r}'.format(
            ftype, read_ftype))
    matchgrp = r_idstamp.match(stamp)
    if not matchgrp:
        raise ValueError(
            u"Expected Exchange type identifier line with stamp (e.g. "
            "{0},YYYYMMDDdivINSwho) got: {1!r}".format(
            ftype, ','.join([ftype, stamp])))

    dfile.globals['stamp'] = stamp
    if not r_stamp.match(stamp):
        LOG.warn(u'{0!r} does not match stamp format YYYYMMDDdivINSwho.'.format(
            stamp))


def read_comments(dfile, fileobj):
    """Read the Exchange header comments.

    Comments are contiguous lines starting with '#'.

    Return: the last line that was read and determined as not a comment.

    """
    line = fileobj.readline()
    headers = []
    while line and line.startswith('#'):
        # It's possible for files to come in with unicode.
        headers.append(line.decode('raw_unicode_escape'))
        line = fileobj.readline()
    dfile.globals['header'] = u''.join(headers)
    return line


def _prepare_to_read_exchange_data(dfile, columns):
    """Return preparatory information about the columns to be read.

    columns - list of WOCE names of parameters

    Returns:
        A list of tuples, each containing the list to which to append the next
        value as well as, depending on whether the column is:
        1. data column
        a standard Parameter that has been loaded from the database with
        format string
        2. flag column
        a tuple including the flag name, the attribute of the Column for the
        flag column, and the parameter name

    """
    infos = []
    ssesh = session()
    for column in columns:
        flag_info = None
        if column.endswith(FLAG_ENDING_WOCE):
            colname = column[:column.index(FLAG_ENDING_WOCE)]
            flag_info = ('WOCE', 'flags_woce', colname)
        elif column.endswith(FLAG_ENDING_IGOSS):
            colname = column[:column.index(FLAG_ENDING_IGOSS)]
            flag_info = ('IGOSS', 'flags_igoss', colname)
        else:
            colname = column
        try:
            col = dfile[colname]
        except KeyError, err:
            if flag_info:
                LOG.error(u'Flag column {0} exists without parameter '
                    'column {1}'.format(column, colname))
            raise err

        if flag_info:
            col = getattr(col, flag_info[1])
            infos.append((col, flag_info))
        else:
            infos.append((col, find_parameter(ssesh, column)))
    return infos


def _read_data_row(dfile, row_i, info, raw):
    raw_value = raw.strip()
    col, param = info
    if type(param) is tuple:
        try:
            value = int(raw_value)
        except (ValueError, TypeError):
            LOG.warn(
                u'Bad {0} flag {1!r} for {2} on data row {3}'.format(
                param[0], raw_value, param[2], row_i))
            value = None
    else:
        if param is None or param.format.endswith('s'):
            value = raw_value
        else:
            if out_of_band(raw_value):
                value = None
            else:
                try:
                    value = _decimal(raw_value)
                except:
                    value = raw_value
    col.append(value)


def read_data(dfile, fileobj, columns):
    """Read Exchange data rows."""
    row_i = 0
    infos = _prepare_to_read_exchange_data(dfile, columns)
    l = fileobj.readline().strip()
    while l:
        if l.startswith(END_DATA):
            break
        values = l.split(',')
        
        # Check columns and values to match length
        if len(columns) != len(values):
            raise ValueError(
                'Expected as many columns as values in file ({0}). Found {1} '
                'columns and {2} values at data line {3}'.format(
                    fileobj.name, len(columns), len(values), len(dfile) + 1))
        for info, raw in zip(infos, values):
            _read_data_row(dfile, row_i, info, raw)
        l = fileobj.readline().strip()
        row_i += 1


def get_flagged_format_parameter_values(dfile):
    """Return a list of tuples containing column format specifics.

    Said specifics include, format string, max column length, parameter and
    values.

    The format string is only really ever used for formatting fill values
    because the data sigfigs should be preserved by Decimal and faithfully
    returned.

    The max column length allows for rjustifying data so it all lines up neatly.

    """
    columns = dfile.sorted_columns()
    flagged_parameter_names = []
    flagged_units = []
    flagged_format_parameter_values = []

    for col in columns:
        param = col.parameter
        flagged_parameter_names.append(param.mnemonic_woce())
        flagged_units.append(param.units.mnemonic if param.units and \
            param.units.mnemonic else '')
        # For columns with data where the decimal places are different from
        # precision given for parameter, use the maximum decimal places found in
        # the data. If no data, then default to the given precision.
        format_str = param.format
        if format_str.endswith('f'):
            parts = format_str[:-1].split('.')
            if len(parts) == 2:
                col_decplaces = col.decimal_places()
                if col_decplaces:
                    decplaces = col_decplaces
                else:
                    decplaces = int(parts[1])
                format_str = '{0}.{1}f'.format(parts[0], decplaces)
        flagged_format_parameter_values.append(
            [format_str, len(format_str % FILL_VALUE), param, col.values])
        if col.is_flagged_woce():
            flagged_parameter_names.append(param.mnemonic_woce() + FLAG_ENDING_WOCE)
            flagged_units.append('')
            flagged_format_parameter_values.append(
                ['%1d', 1, param, col.flags_woce])
        if col.is_flagged_igoss():
            flagged_parameter_names.append(param.mnemonic_woce() + FLAG_ENDING_IGOSS)
            flagged_units.append('')
            flagged_format_parameter_values.append(
                ['%1d', 1, param, col.flags_igoss])
    return flagged_parameter_names, flagged_units, flagged_format_parameter_values


def write_flagged_format_parameter_values(dfile, fileobj,
                                          flagged_format_parameter_values):
    for i in range(len(dfile)):
        values = []
        for format_str, limit, param, col in flagged_format_parameter_values:
            try:
                value = col[i]
            except IndexError, err:
                LOG.error(u'Could not get value of {0} at row {1}'.format(
                    param, i))
                value = None
            if value is None:
                value = format_str % FILL_VALUE
            try:
                values.append(decimal_to_str(value).rjust(limit))
            except Exception, err:
                LOG.warn(
                    u'Could not format {0} (column {1} row {2:d}): {3}'.format(
                    value, param, i, err))
                values.append(value)
        fileobj.write(','.join(values) + '\n')


def write_identifier(dfile, fileobj, ftype):
    """Write the file type identifier. E.g. BOTTLE/CTD + stamp."""
    try:
        stamp = dfile.globals['stamp']
    except KeyError:
        stamp = None
    if not stamp:
        stamp  = user_stamp()
    assert ftype in ['BOTTLE', 'CTD']
    fileobj.write(u'{0},{1}\n'.format(ftype, stamp))


def write_data(dfile, fileobj):
    """Write columns of data."""
    flagged_parameter_names, flagged_units, flagged_format_parameter_values = \
        get_flagged_format_parameter_values(dfile)

    fileobj.write(','.join(flagged_parameter_names) + '\n')
    fileobj.write(','.join(flagged_units) + '\n')

    write_flagged_format_parameter_values(
        dfile, fileobj, flagged_format_parameter_values)

    fileobj.write(END_DATA + '\n')
