"""libcchdo.formats.ctd.woce"""

import libcchdo
import libcchdo.formats.woce as fmtwoce
import re
import datetime


def read(self, handle):
    """How to read a CTD WOCE file."""
    # TODO Out of band values should be converted to None
    # Get the stamp
    stamp = re.compile(
        ('EXPOCODE\s*([\w/]+)\s*WHP.?IDS?\s*([\w/]+(,[\w/]+)?)\s*'
         'DATE\s*(\d{6})'),
        re.IGNORECASE)
    m = stamp.match(handle.readline())
    if m:
      self.globals['EXPOCODE'] = m.group(1)
      self.globals['SECT_ID'] = m.group(2)
      self.globals['DATE'], self.globals['TIME'] = \
          fmtwoce.strftime_woce_date_time(datetime.datetime.strptime(
                                              m.group(len(m.groups())), '%m%d%y'))
    else:
      raise ValueError("Expected stamp. Invalid record 1 in WOCE CTD file.")
    # Get identifier line
    identifier = re.compile(
        'STNNBR\s*(\d+)\s*CASTNO\s*(\d+)\s*NO\. Records=\s*(\d+)', re.IGNORECASE)
    m = identifier.match(handle.readline())
    if m:
      self.globals['STNNBR'] = m.group(1)
      self.globals['CASTNO'] = m.group(2)
    else:
      raise ValueError(("Expected identifiers. Invalid record 2 in "
                        "WOCE CTD file."))

    # Get instrument line
    instrument = re.compile(
        'INSTRUMENT NO.\s*(\d+)\s*SAMPLING RATE\s*(\d+.\d+\s*HZ)', re.IGNORECASE)
    m = instrument.match(handle.readline())
    if m:
      self.globals['_INSTRUMENT_NO'] = m.group(1)
      self.globals['_SAMPLING_RATE'] = m.group(2)
    else:
      raise ValueError(("Expected instrument information. "
                        "Invalid record 3 in WOCE CTD file."))
    
    parameters_line = handle.readline()
    units_line = handle.readline()
    asterisk_line = handle.readline()

    self.read_WOCE_data(handle, parameters_line, units_line, asterisk_line)

def write(self, handle):
    '''How to write a CTD WOCE file.'''
    # We can only write the CTD file if there is a unique
    # EXPOCODE, STNNBR, and CASTNO in the file.
    expocodes = self.globals["EXPOCODE"] #self.expocodes()
    sections = self.globals["SECT_ID"] #libcchdo.uniquify(self.columns['SECT_ID'].values)
    stations = self.globals["STNNBR"] #libcchdo.uniquify(self.columns['STNNBR'].values)
    casts = self.globals["CASTNO"] #libcchdo.uniquify(self.columns['CASTNO'].values)

    #def has_multiple_values(a):
    #    return len(a) is not 1

    #if any(map(has_multiple_values, [expocodes, sections, stations, casts])):
    #  raise ValueError(('Cannot write a multi-ExpoCode, section, station, '
    #                    'or cast WOCE CTD file.'))
    #else:
    #  expocode = expocodes[0]
    #  section = sections[0]
    #  station = stations[0]
    #  cast = casts[0]

    expocode = expocodes # XXX
    section = sections   # XXX
    station = stations   # XXX
    cast = casts         # XXX

    handle.write('EXPOCODE %-14s WHP-ID %-5s DATE %-6d\n' % \
                 (expocode, section, int(self.globals["DATE"])))
    # 2 at end of line denotes record 2
    handle.write('STNNBR %-8s CASTNO %-3d NO. RECORDS=%-5d%s\n' %
                 (station, int(cast), len(self.columns), ""))
    # 3 denotes record 3
    handle.write('INSTRUMENT NO. %-5s SAMPLING RATE %-6.2f HZ%s\n' %
                 (0, 42.0, ""))
    #handle.write('  CTDPRS  CTDTMP  CTDSAL  CTDOXY  NUMBER QUALT1') # TODO
    #handle.write('    DBAR  ITS-90  PSS-78 UMOL/KG    OBS.      *') # TODO
    #handle.write(' ******* ******* ******* *******              *') # TODO
    #handle.write('     3.0 28.7977 31.8503   209.5      42   2222') # TODO

    def parameter_name_of(column):
        return column.parameter.woce_mnemonic

    def units_of(column):
        return column.parameter.units_mnemonic

    def flags_for(column):
        return "*******" if column.is_flagged_woce() else ""

    base_format = "%8s" * len(self.columns)
    num_flags = sum( map(lambda column: 1 if flags_for(column) else 0,
            self.columns.values() ) )
    if num_flags != 0:
        base_format += "%%%ds" % (max(len("QUALT#"), num_flags) + 1)
    base_format += "\n"

    parameter_header_format = \
            units_header_format = \
            asterisks_header_format = base_format

    columns = self.sorted_columns()
    print map(parameter_name_of, columns)
    print map(lambda col: col.is_flagged_woce(), columns)
    qualt_colsize = len(" QUALT#") + \
            0 if num_flags < " QUALT#" else num_flags - len(" QUALT#")
    qualt_spaces = " " * (qualt_colsize - len("QUALT#"))

    all_headers = map(parameter_name_of, columns)
    all_headers.append(qualt_spaces + "QUALT1")

    all_units = map(units_of, columns)
    all_units.append(" " * (qualt_colsize - 1) + "*")

    all_asters = map(flags_for, columns)
    all_asters.append(" " * (qualt_colsize - 1) + "*")

    handle.write(parameter_header_format % tuple(all_headers))
    handle.write(units_header_format %  tuple(all_units))
    handle.write(asterisks_header_format %  tuple(all_asters))
