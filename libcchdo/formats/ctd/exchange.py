from re import compile as re_compile, sub as re_sub
from collections import OrderedDict

from libcchdo.log import LOG
from libcchdo.fns import Decimal, out_of_band, decimal_to_str
from libcchdo.recipes.orderedset import OrderedSet
from libcchdo.formats import pre_write
from libcchdo.formats import woce
from libcchdo.formats.exchange import (
    FLAG_ENDING_WOCE, FLAG_ENDING_IGOSS,
    read_identifier_line, read_comments, write_identifier, write_data,
    write_flagged_format_parameter_values, FILL_VALUE, END_DATA)
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = ['_ct1.csv', 'ct1.csv']


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


def get_datafile_filename(dfile):
    """Returns an Exchange CTD filename identifier given a DataFile."""
    expocode = dfile.globals['EXPOCODE']
    station = dfile.globals['STNNBR'].strip()
    cast = dfile.globals['CASTNO'].strip()

    try:
        station = '%05d' % int(station)
    except TypeError:
        station = station[:5]
    try:
        cast = '%05d' % int(cast)
    except TypeError:
        cast = cast[:5]
    filename = '%s_%5s_%5s' % (expocode, station, cast)
    filename = re_sub('\s', '_', filename)
    return get_filename(filename)


REQUIRED_HEADERS = [
    u'EXPOCODE', u'SECT_ID', u'STNNBR', u'CASTNO', u'DATE', u'TIME',
    u'LATITUDE', u'LONGITUDE', u'DEPTH', ]


def read(self, handle, retain_order=False, header_only=False):
    """How to read a CTD Exchange file.

    header_only - only read the CTD headers, not the data

    """
    read_identifier_line(self, handle, 'CTD')
    l = read_comments(self, handle)

    # Read NUMBER_HEADERS
    num_headers = re_compile('NUMBER_HEADERS\s*=\s*(\d+)')
    m = num_headers.match(l)
    if m:
         # NUMBER_HEADERS counts itself as a header
        num_headers = int(m.group(1))-1
    else:
        raise ValueError(
            u'Expected NUMBER_HEADERS as the second non-comment line.')
    header = re_compile('(\w+)\s*=\s*(-?[\w\.]*)')
    for i in range(0, num_headers):
        m = header.match(handle.readline())
        if m:
            if m.group(1) in REQUIRED_HEADERS and m.group(1) in ['LATITUDE',
                                                                 'LONGITUDE']:
                self.globals[m.group(1)] = Decimal(m.group(2))
            else:
                self.globals[m.group(1)] = m.group(2)
        else:
            raise ValueError(('Expected %d continuous headers '
                              'but only saw %d') % (num_headers, i))
    woce.fuse_datetime(self)

    if header_only:
        return

    # Read parameters and units
    columns = handle.readline().strip().split(',')
    units = handle.readline().strip().split(',')
    
    # Check columns and units to match length
    if len(columns) is not len(units):
        raise ValueError(("Expected as many columns as units in file. "
                          "Found %d columns and %d units.") % \
                         (len(columns), len(units)))

    # Check all parameters are non-trivial
    if not all(columns):
        LOG.warn(("Stripped blank parameter from MALFORMED EXCHANGE FILE\n"
                  "This may be caused by an extra comma at the end of a line."))
        columns = filter(None, columns)

    self.create_columns(columns, units, retain_order)

    # Read data
    numberlike = re_compile('-?\d+(.\d+)?([eE]-?\d+)?')
    l = handle.readline().strip()
    while l:
        if l == END_DATA:
            break
        values = l.split(',')
        
        # Check columns and values to match length
        if len(columns) is not len(values):
            raise ValueError(
                ("Expected as many columns as values in file (%s). Found %d "
                 "columns and %d values at data line %d") % \
                 (handle.name, len(columns), len(values), len(self) + 1))

        for column, value in zip(columns, values):
            value = value.strip()
            if column.endswith(FLAG_ENDING_WOCE):
                self.columns[column[:-7]].flags_woce.append(int(value))
                continue
            elif column.endswith(FLAG_ENDING_IGOSS):
                self.columns[column[:-7]].flags_igoss.append(int(value))
                continue
            if out_of_band(float(value)):
                self.columns[column].append(None)
                continue

            if numberlike.match(value):
                value = Decimal(str(value))
            col = self.columns[column]
            col.append(value)
        l = handle.readline().strip()

    self.check_and_replace_parameters()


def write(self, handle):
    """ How to write a CTD Exchange file. """
    pre_write(self)

    write_identifier(self, handle, 'CTD')
    if self.globals['header']:
        handle.write(self.globals['header'].encode('utf8'))

    # Collect headers
    headers = OrderedDict()
    headers['NUMBER_HEADERS'] = 1

    woce.split_datetime(self)
    for key in REQUIRED_HEADERS:
        try:
            headers[key] = self.globals[key]
        except KeyError:
            LOG.error('Missing required header %s' % key)
    keys_less_required = OrderedSet(self.globals.keys()) - \
                         set(['stamp', 'header']) - \
                         set(REQUIRED_HEADERS)
    for key in keys_less_required:
        headers[key] = self.globals[key]
    headers['NUMBER_HEADERS'] = len(headers)
    woce.fuse_datetime(self)

    # Write headers
    for key in headers:
        handle.write(u'{key} = {val}\n'.format(
            key=key, val=decimal_to_str(headers[key])))

    write_data(self, handle)
