from libcchdo.fns import _decimal
from libcchdo.log import LOG
from libcchdo.model.datafile import Column
from libcchdo.formats import woce
from libcchdo.formats.exchange import (
    FLAG_ENDING_WOCE, FLAG_ENDING_IGOSS,
    read_identifier_line, read_comments, read_data, write_identifier,
    write_data, write_flagged_format_parameter_values, FILL_VALUE, END_DATA)
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

    read_data(self, handle, columns)

    # Format all data to be what it is
    try:
        self['EXPOCODE'].values = map(str, self['EXPOCODE'].values)
    except KeyError:
        pass
    try:
        self['LATITUDE'].values = map(_decimal, self['LATITUDE'].values)
    except KeyError:
        pass
    try:
        self['LONGITUDE'].values = map(_decimal, self['LONGITUDE'].values)
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
    write_identifier(self, handle, 'BOTTLE')
    if self.globals['header']:
        handle.write('# Original header:\n')
        handle.write(self.globals['header'].encode('utf8'))

    woce.split_datetime(self)

    # Convert all float stnnbr, castno, sampno, btlnbr to ints
    def if_float_then_int(x):
        if type(x) is float:
            return int(x)
        return x

    def convert_column_floats_to_ints(dfile, param, required=True):
        try:
            column = dfile[param]
            column.values = [if_float_then_int(vvv) for vvv in column.values]
        except KeyError:
            if required:
                LOG.warn(u'Missing {0} column'.format(param))
            else:
                LOG.warn(u'Missing optional {0} column'.format(param))

    convert_column_floats_to_ints(self, 'STNNBR')
    convert_column_floats_to_ints(self, 'CASTNO')
    convert_column_floats_to_ints(self, 'SAMPNO', required=False)
    convert_column_floats_to_ints(self, 'BTLNBR')
    self.check_and_replace_parameters()

    write_data(self, handle)

    woce.fuse_datetime(self)
