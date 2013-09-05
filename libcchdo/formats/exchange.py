"""Exchange related functions.

"""
from re import compile as re_compile, match as re_match

from libcchdo.config import stamp as user_stamp
from libcchdo.log import LOG
from libcchdo.fns import Decimal


# Where no data is known
FILL_VALUE = -999.0


FLAG_ENDING_WOCE = '_FLAG_W'
FLAG_ENDING_IGOSS = '_FLAG_I'


END_DATA = 'END_DATA'


r_idstamp = re_compile('(BOTTLE|CTD),(\w+)')
r_stamp = re_compile('\d{8}\w+')


def read_identifier_line(dfile, fileobj, ftype):
    """Read an Exchange identifier line from the fileobj to the dfile.

    An Exchange identifier line begins with either "BOTTLE," or "CTD," and ends
    with a WOCE style stamp.

    Raises:
        ValueError - if either the file type is not one of BOTTLE or CTD or the
            identifier line is malformed.

    """
    stamp_line = fileobj.readline()
    matchgrp = r_idstamp.match(stamp_line)
    if not matchgrp:
        raise ValueError(
            u"Expected Exchange type identifier line with stamp (e.g. "
            "{0},YYYYMMDDdivINSwho) got: {1!r}".format(ftype, stamp_line))

    read_ftype = matchgrp.group(1)
    if ftype != read_ftype:
        raise ValueError(
            u'Expected Exchange file type {0!r} and got {1!r}'.format(
            ftype, read_ftype))
    stamp = dfile.globals['stamp'] = matchgrp.group(2)
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
                values.append(str(value).rjust(limit))
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

    fileobj.write(END_DATA)
