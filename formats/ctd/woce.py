''' libcchdo.ctd.woce '''

from format import format

class woce(format):
  def read(self, handle):
    '''How to read a CTD WOCE file.'''
    # TODO Out of band values should be converted to None
    # Get the stamp
    stamp = compile('EXPOCODE\s*([\w/]+)\s*WHP.?IDS?\s*([\w/]+(,[\w/]+)?)\s*DATE\s*(\d{6})', re.IGNORECASE)
    m = stamp.match(handle.readline())
    if m:
      self.globals['EXPOCODE'] = m.group(1)
      self.globals['SECT_ID'] = m.group(2)
      self.globals['DATE'], self.globals['TIME'] = strftime_woce_date_time(datetime.strptime(m.group(3), '%m%d%y'))
    else:
      raise ValueError("Expected stamp. Invalid record 1 in WOCE CTD file.")
    # Get identifier line
    identifier = compile('STNNBR\s*(\d+)\s*CASTNO\s*(\d+)\s*NO\. Records=\s*(\d+)')
    m = identifier.match(handle.readline())
    if m:
      self.globals['STNNBR'] = m.group(1)
      self.globals['CASTNO'] = m.group(2)
    else:
      raise ValueError("Expected identifiers. Invalid record 2 in WOCE CTD file.")

    # Get instrument line
    instrument.compile('INSTRUMENT NO.\s*(\d+)\s*SAMPLING RATE\s*(\d+.\d+\s*HZ)')
    m = instrument.match(handle.readline())
    if m:
      self.globals['_INSTRUMENT_NO'] = m.group(1)
      self.globals['_SAMPLING_RATE'] = m.group(2)
    else:
      raise ValueError("Expected instrument information. Invalid record 3 in WOCE CTD file.")
    
    parameters_line = handle.readline()
    units_line = handle.readline()
    asterisk_line = handle.readline()

    self.read_WOCE_data(handle, parameters_line, units_line, asterisk_line)
  def write(self, handle):
    '''How to write a CTD WOCE file.'''
    # We can only write the CTD file if there is a unique EXPOCODE, STNNBR, and CASTNO in the file.
    expocodes = self.expocodes()
    sections = uniquify(self.columns['SECT_ID'].values)
    stations = uniquify(self.columns['STNNBR'].values)
    casts = uniquify(self.columns['CASTNO'].values)
    if len(expocodes) is not len(sections) is not len(stations) is not len(casts) is not 1:
      raise ValueError('Cannot write a multi-ExpoCode, section, station, or cast WOCE CTD file.')
    else:
      expocode = expocodes[0]
      section = sections[0]
      station = stations[0]
      cast = casts[0]
    handle.write('EXPOCODE %14s WHP-ID %5s DATE %6d' % (expocode, section, date))
    handle.write('STNNBR %8s CASTNO %3d NO. RECORDS=%5d%s2') # 2 denotes record 2
    handle.write('INSTRUMENT NO. %5s SAMPLING RATE %6.2f HZ%s3') # 3 denotes record 3
    handle.write('  CTDPRS  CTDTMP  CTDSAL  CTDOXY  NUMBER QUALT1') # TODO
    handle.write('    DBAR  ITS-90  PSS-78 UMOL/KG    OBS.      *') # TODO
    handle.write(' ******* ******* ******* *******              *') # TODO
    handle.write('     3.0 28.7977 31.8503   209.5      42   2222') # TODO
