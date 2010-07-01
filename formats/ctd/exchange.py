'''libcchdo.formats.ctd.exchange'''

from re import compile
from datetime import datetime, date


def read(self, handle):
    '''How to read a CTD Exchange file.'''
    # Read identifier and stamp
    stamp = compile('CTD,(\d{8}\w+)')
    m = stamp.match(handle.readline())
    if m:
        self.stamp = m.group(1)
    else:
        raise ValueError('Expected identifier line with stamp (e.g. CTD,YYYYMMDDdivINSwho)')
    # Read comments
    l = handle.readline()
    while l and l.startswith('#'):
        self.header += l
        l = handle.readline()
    # Read NUMBER_HEADERS
    num_headers = compile('NUMBER_HEADERS\s+=\s+(\d+)')
    m = num_headers.match(l)
    if m:
        num_headers = int(m.group(1))-1 # NUMBER_HEADERS counts itself as a header
    else:
        raise ValueError('Expected NUMBER_HEADERS as the second line in the file.')
    header = compile('(\w+)\s*=\s*(-?\w+)')
    for i in range(0, num_headers):
        m = header.match(handle.readline())
        if m:
            self.globals[m.group(1)] = m.group(2)
        else:
            raise ValueError('Expected %d continuous headers but only saw %d' % (num_headers, i))
    # Read parameters and units
    columns = handle.readline().strip().split(',')
    units = handle.readline().strip().split(',')
    
    # Check columns and units to match length
    if len(columns) is not len(units):
        raise ValueError("Expected as many columns as units in file. Found %d columns and %d units." % (len(columns), len(units)))

    self.create_columns(columns, units)

    # Read data
    l = handle.readline().strip()
    while l:
      if l == 'END_DATA':
          break
      values = l.split(',')
      
      # Check columns and values to match length
      if len(columns) is not len(values):
          raise ValueError("Expected as many columns as values in file. Found %d columns and %d values at data line %d" % (len(columns), len(values), len(self)+1))
      for column, value in zip(columns, values):
          value = value.strip()
          if column.endswith('_FLAG_W'):
              self.columns[column[:-7]].flags_woce.append(value)
          elif column.endswith('_FLAG_I'):
              self.columns[column[:-7]].flags_igoss.append(value)
          else:
              self.columns[column].append(value)
      l = handle.readline().strip()


def write(self, handle):
    '''How to write a CTD Exchange file.'''
    today = date.today()
    handle.write('CTD,%4d%02d%02dSIOCCHDLIB\n' % (today.year, today.month, today.day))
    handle.write(self.header)
    handle.write('NUMBER_HEADERS = '+str(len(self.globals.keys())+1)+"\n")
    required_headers = ('EXPOCODE', 'SECT', 'STNNBR', 'CASTNO', 'DATE',
                        'TIME', 'LATITUDE', 'LONGITUDE', 'DEPTH')
    for header in required_headers:
        handle.write(header+' = '+str(self.globals[header])+"\n")
    for key in set(self.globals.keys()) - set(required_headers):
        handle.write(key+' = '+str(self.globals[key])+"\n")

    headers = []
    for c in self.sorted_columns():
        param = c.parameter.woce_mnemonic
        headers.append(param)
        if c.is_flagged_woce():
            headers.append(param+'_FLAG_W')
        if c.is_flagged_igoss():
            headers.append(param+'_FLAG_I')
    handle.write(','.join(headers)+"\n")
    columns = [self.columns[header] for header in self.column_headers()]
    for i in range(len(self)):
        data = []
        for c in columns:
            data.append(('%'+c.parameter.format) % float(c[i]) if c[i] else -999)
            if c.is_flagged_woce():
                data.append(c.flags_woce[i])
            if c.is_flagged_igoss():
                data.append(c.flags_igoss[i])
        handle.write(','.join(map(str, data))+"\n")
    handle.write("END_DATA\n")
