import re
import datetime
import decimal

from ... import LOG
from ... import fns
from .. import pre_write


REQUIRED_HEADERS = ('EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO', 'DATE',
                    'TIME', 'LATITUDE', 'LONGITUDE', 'DEPTH', )


def read(self, handle, retain_order=False):
    """ How to read a CTD Exchange file. """
    # Read identifier and stamp
    stamp = re.compile('CTD,(\d{8}\w+)')
    stampline = handle.readline()
    m = stamp.match(stampline)
    if m:
        self.globals['stamp'] = m.group(1)
    else:
        raise ValueError(('Expected identifier line with stamp '
                          '(e.g. CTD,YYYYMMDDdivINSwho).\n'
                          'Instead got: %s') % stampline)
    # Read comments
    l = handle.readline()
    while l and l.startswith('#'):
        self.globals['header'] += l.strip() + '\n'
        l = handle.readline()

    # Read NUMBER_HEADERS
    num_headers = re.compile('NUMBER_HEADERS\s*=\s*(\d+)')
    m = num_headers.match(l)
    if m:
         # NUMBER_HEADERS counts itself as a header
        num_headers = int(m.group(1))-1
    else:
        raise ValueError(('Expected NUMBER_HEADERS as the second line in '
                          'the file.'))
    header = re.compile('(\w+)\s*=\s*(-?[\w\.]+)')
    for i in range(0, num_headers):
        m = header.match(handle.readline())
        if m:
            if m.group(1) in REQUIRED_HEADERS and m.group(1) in ['LATITUDE',
                                                                 'LONGITUDE']:
                self.globals[m.group(1)] = decimal.Decimal(m.group(2))
            else:
                self.globals[m.group(1)] = m.group(2)
        else:
            raise ValueError(('Expected %d continuous headers '
                              'but only saw %d') % (num_headers, i))
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
    numberlike = re.compile('-?\d+(.\d+)?([eE]-?\d+)?')
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
            if fns.out_of_band(float(value)):
                self.columns[column].append(None)
                continue

            if numberlike.match(value):
                value = decimal.Decimal(str(value))
            col = self.columns[column]
            col.append(value)
        l = handle.readline().strip()

    self.check_and_replace_parameters()


def write(self, handle):
    """ How to write a CTD Exchange file. """
    pre_write(self)

    handle.write('CTD,%s\n' % self.globals['stamp'])
    handle.write('%s' % self.globals['header'])

    stamp = self.globals['stamp']
    header = self.globals['header']
    del self.globals['stamp']
    del self.globals['header']

    handle.write('NUMBER_HEADERS = '+str(len(self.globals.keys())+1)+"\n")
    for header in REQUIRED_HEADERS:
        try:
            handle.write(header+' = '+str(self.globals[header])+"\n")
        except KeyError:
            LOG.warn('Missing required header %s' % header)
    for key in set(self.globals.keys()) - set(REQUIRED_HEADERS):
        handle.write(key+' = '+str(self.globals[key])+"\n")


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
    handle.write(','.join(headers)+"\n")

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
    handle.write(",".join(units)+"\n")
    #XXX

    columns = [self.columns[header] for header in self.column_headers()]
    for i in range(len(self)):
        data = []
        for c in columns:
            fmt = c.parameter.format
            if fmt.endswith('f'):
                parts = fmt[:-1].split('.')
                assert len(parts) <=2 and len(parts) >= 1
                if len(parts) == 2:
                    # Should do this check before printing to prevent ragged columns.
                    if type(c[i]) is decimal.Decimal:
                        exponent = \
                            -(c[i] - c[i].to_integral()).as_tuple().exponent
                        if exponent > -1 and exponent > parts[1]:
                            fmt = '%%%d.%df' % (parts[0], exponent)
            try:
                string = c.parameter.format % (c[i] if c[i] else -999.0)
            except TypeError:
                print type(c[i]), c[i]
                raise

            data.append(string)
            if c.is_flagged_woce():
                data.append(c.flags_woce[i])
            if c.is_flagged_igoss():
                data.append(c.flags_igoss[i])
        handle.write(','.join(map(str, data))+"\n")
    handle.write("END_DATA\n")
