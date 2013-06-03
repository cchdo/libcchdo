from re import compile as re_compile

from libcchdo import config
from libcchdo.log import LOG
from libcchdo.fns import Decimal, out_of_band
from libcchdo.formats import pre_write
from libcchdo.formats import woce
from libcchdo.formats.exchange import (
    read_identifier_line, read_comments, FILL_VALUE, END_DATA)


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
        if l == 'END_DATA':
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
            if column.endswith('_FLAG_W'):
                self.columns[column[:-7]].flags_woce.append(int(value))
                continue
            elif column.endswith('_FLAG_I'):
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

    handle.write(u'CTD,%s\n' % config.stamp())
    handle.write(self.globals['header'].encode('utf8'))

    stamp = self.globals['stamp']
    header = self.globals['header']
    del self.globals['stamp']
    del self.globals['header']

    woce.split_datetime(self)
    handle.write(u'NUMBER_HEADERS = '+str(len(self.globals.keys())+1)+"\n")
    for header in REQUIRED_HEADERS:
        try:
            handle.write(header+' = '+str(self.globals[header])+"\n")
        except KeyError:
            LOG.warn('Missing required header %s' % header)

    for key in set(self.globals.keys()) - set(REQUIRED_HEADERS):
        handle.write(u'{key} = {val}\n'.format(key=key, val=self.globals[key]))
    woce.fuse_datetime(self)

    self.globals['stamp'] = stamp
    self.globals['header'] = header

    headers = []
    for c in self.sorted_columns():
        param = c.parameter.mnemonic_woce()
        headers.append(param)
        if c.is_flagged_woce():
            headers.append(param+'_FLAG_W')
        if c.is_flagged_igoss():
            headers.append(param+'_FLAG_I')
    handle.write(u','.join(headers)+"\n")

    #XXX
    units = []
    for c in self.sorted_columns():
        if c.parameter.units:
            u = c.parameter.units.mnemonic
            units.append(u)
        else:
            units.append('')
        if c.is_flagged():
            units.append('')
    handle.write(u",".join(units)+"\n")
    #XXX

    columns = [self.columns[header] for header in self.column_headers()]
    for i in range(len(self)):
        data = []
        for c in columns:
            fmt = c.parameter.format
            if fmt.endswith('f'):
                parts = fmt[:-1].split('.')
                assert len(parts) <= 2 and len(parts) >= 1
                if len(parts) == 2:
                    # Should do this check before printing to prevent ragged columns.
                    if type(c[i]) is Decimal:
                        exponent = \
                            -(c[i] - c[i].to_integral()).as_tuple()[-1]
                        if exponent > -1 and exponent > parts[1]:
                            fmt = '%%%d.%df' % (parts[0], exponent)
            try:
                if c[i] is not None:
                    value = c[i]
                else:
                    value = FILL_VALUE
                string = c.parameter.format % value
            except TypeError:
                LOG.debug(u'{0} {1}'.format(type(c[i]), c[i]))
                raise

            data.append(string)
            if c.is_flagged_woce():
                data.append(c.flags_woce[i])
            if c.is_flagged_igoss():
                data.append(c.flags_igoss[i])
        handle.write(u','.join(map(str, data))+"\n")
    handle.write(unicode(END_DATA))
