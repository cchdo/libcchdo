import re
import datetime

from libcchdo import config
from libcchdo.fns import _decimal, out_of_band
from libcchdo.log import LOG
from libcchdo.model.datafile import Column
from libcchdo.formats import woce
from libcchdo.formats.exchange import (
    read_identifier_line, read_comments, FILL_VALUE, END_DATA)
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


# TM and LV refer to Trace Metals and Large Volume respectively
_fname_extensions = [
    '_hy1.csv', 'hy1.csv', 'lv_hy1.csv', 'tm_hy1.csv', '.exc.csv']


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
    """ How to read a Bottle Exchange file. """
    read_identifier_line(self, handle, 'BOTTLE')
    l = read_comments(self, handle)

    # Read columns and units
    columns = [x.strip() for x in l.strip().split(',')]
    units = [x.strip() for x in handle.readline().strip().split(',')]
    
    # Check columns and units to match length
    if len(columns) is not len(units):
        raise ValueError(("Expected as many columns as units in file. "
                          "Found %d columns and %d units.") % (len(columns),
                                                               len(units)))

    # Check for unique identifer
    identifier = []
    if 'EXPOCODE' in columns and \
       'STNNBR' in columns and \
       'CASTNO' in columns:
        identifier = ['STNNBR', 'CASTNO']
        if 'SAMPNO' in columns:
            identifier.append('SAMPNO')
            if 'BTLNBR' in columns:
                identifier.append('BTLNBR')
        elif 'BTLNBR' in columns:
            identifier.append('BTLNBR')
        else:
            raise ValueError(
                ("No unique identifer found for file. "
                 "(STNNBR,CASTNO,SAMPNO,BTLNBR),"
                 "(STNNBR,CASTNO,SAMPNO),"
                 "(STNNBR,CASTNO,BTLNBR)"))

    self.create_columns(columns, units)

    # Read data
    row_i = 0
    l = handle.readline().strip()
    while l:
        if l.startswith(END_DATA): break
        values = l.split(',')
        
        # Check columns and values to match length
        if len(columns) is not len(values):
            raise ValueError(("Expected as many columns as values in file. "
                              "Found %d columns and %d values at "
                              "data line %d") % (len(columns), len(values),
                                                len(self) + 1))

        # TODO check if parameter exists but no flag & vice versa

        for column, raw in zip(columns, values):
            raw_value = raw.strip()
            if out_of_band(raw_value):
                value = None
            else:
                try:
                    value = _decimal(raw_value)
                except:
                    value = raw_value

            param_name = column[:-7]
            flag_column = None
            flag_type = None
            if column.endswith('_FLAG_W'):
                flag_column = self[param_name].flags_woce
                flag_type = 'WOCE'
            elif column.endswith('_FLAG_I'):
                flag_column = self[param_name].flags_igoss
                flag_type = 'IGOSS'
            else:
                self[column].append(value)

            if flag_column is not None:
                try:
                    flag_column.append(int(value))
                except TypeError:
                    LOG.warn(
                        u'Flag {0} for parameter {1} has bad flag {2!r} on '
                        'data line {3}'.format(
                        flag_type, param_name, raw_value, row_i))
                    flag_column.append(None)
                except KeyError:
                    LOG.warn(
                        u'Flag {0} column exists for parameter {1} but '
                        'parameter column does not exist.'.format(
                        flag_type, param_name))
                    flag_column.append(None)
        l = handle.readline().strip()
        row_i += 1

    # Format all data to be what it is
    try:
        self['LATITUDE'].values = map(float, self['LATITUDE'].values)
    except KeyError:
        pass
    try:
        self['LONGITUDE'].values = map(float, self['LONGITUDE'].values)
    except KeyError:
        pass
    try:
        self['DATE']
    except KeyError:
        self['DATE'] = Column('DATE')
        self['DATE'].values = [None] * len(self)
    try:
        self['TIME']
    except KeyError:
        self['TIME'] = Column('TIME')
        self['TIME'].values = [None] * len(self)

    woce.fuse_datetime(self)

    self.check_and_replace_parameters()


def write(self, handle):
    """ How to write a Bottle Exchange file. """
    if self.globals['stamp']:
        handle.write('BOTTLE,%s\n' % self.globals['stamp'])
    else:
        LOG.warning("No stamp given. Using current user's stamp.")
        stamp = config.stamp()
        handle.write('BOTTLE,%s\n' % stamp)
    if self.globals['header']:
        handle.write('# Original header:\n')
        handle.write(self.globals['header'].encode('utf8'))

    woce.split_datetime(self)

    # Convert all float stnnbr, castno, sampno, btlnbr to ints
    def if_float_then_int(x):
        if type(x) is float:
            return int(x)
        return x

    self['STNNBR'].values = map(if_float_then_int, self['STNNBR'].values)
    self['CASTNO'].values = map(if_float_then_int, self['CASTNO'].values)
    try:
        self['SAMPNO'].values = map(if_float_then_int, self['SAMPNO'].values)
    except KeyError:
        LOG.warn(u'Missing SAMPNO')
        pass
    try:
        self['BTLNBR'].values = map(if_float_then_int, self['BTLNBR'].values)
    except KeyError:
        LOG.warn(u'Missing BTLNBR')
        pass
    self.check_and_replace_parameters()

    columns = self.sorted_columns()
    flagged_parameter_names = []
    flagged_units = []
    flagged_format_parameter_values = []

    for c in columns:
        param = c.parameter
        flagged_parameter_names.append(param.mnemonic_woce())
        flagged_units.append(param.units.mnemonic if param.units and \
            param.units.mnemonic else '')
        flagged_format_parameter_values.append(
            [param.format, len(param.format % FILL_VALUE), param,
             c.values])
        if c.is_flagged_woce():
            flagged_parameter_names.append(param.mnemonic_woce() + '_FLAG_W')
            flagged_units.append('')
            flagged_format_parameter_values.append(
                ['%1d', 1, param, c.flags_woce])
        if c.is_flagged_igoss():
            flagged_parameter_names.append(param.mnemonic_woce() + '_FLAG_I')
            flagged_units.append('')
            flagged_format_parameter_values.append(
                ['%1d', 1, param, c.flags_igoss])

    handle.write(','.join(flagged_parameter_names))
    handle.write('\n')
    handle.write(','.join(flagged_units))
    handle.write('\n')

    for i in range(len(self)):
        values = []
        for format_str, limit, param, col in flagged_format_parameter_values:
            try:
                value = col[i]
            except IndexError, err:
                LOG.error(u'Could not get value of {0} at row {1}'.format(
                    param, i))
                value = None
            try:
                if value is not None:
                    values.append((format_str % value).rjust(limit))
                else:
                    values.append(format_str % FILL_VALUE)
            except Exception, e:
                LOG.warn(
                    u'Could not format %r (column %r row %d) with %r' % (
                    value, param, i, format_str))
                values.append(value)
        handle.write(','.join(values))
        handle.write('\n')

    handle.write(END_DATA)

    woce.fuse_datetime(self)
