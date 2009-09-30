# libcchdo Python
#
# Dependencies
# netcdf4-python - http://code.google.com/p/netcdf4-python/
#   netcdf
#   numpy - http://numpy.scipy.org/
# pygresql - http://www.pygresql.org/
#   postgresql server binaries
# MySQLdb - http://sourceforge.net/projects/mysql-python
#   MySQL server binaries
# 

from __future__ import with_statement
try:
  from netCDF3 import Dataset
except ImportError, e:
  print e, "\n", 'You should get netcdf4-python from http://code.google.com/p/netcdf4-python and install the NetCDF 3 module as directed by the README.'
  exit(1)
try:
  import pgdb
except ImportError, e:
  print e, "\n", 'You should get pygresql from http://www.pygresql.org/readme.html#where-to-get. You will need Postgresql with server binaries installed already.'
  exit(1)
try:
  import MySQLdb
except ImportError, e:
  print e, "\n", 'You should get MySQLdb from http://sourceforge.net/projects/mysql-python. You will need MySQL with server binaries installed already.'
  exit(1)
from datetime import date, datetime
from numpy import dtype
from math import sin, cos, pow
from os import listdir, remove, rmdir
from os.path import exists
from re import compile
from sys import exit
from StringIO import StringIO
from tempfile import mkdtemp
from zipfile import ZipFile, ZipInfo
try:
  from math import isnan
except ImportError:
  def isnan(n):
    return n != n

def connect_postgresql():
  try:
    return pgdb.connect(user='libcchdo',
                        password='((hd0hydr0d@t@',
                        host='goship.ucsd.edu',
                        database='cchdotest')
  except pgdb.Error, e:
    print "Database error: %s" % e
    exit(1)
def connect_mysql():
  try:
    return MySQLdb.connect(user='libcchdo',
                           passwd='((hd0hydr0d@t@',
                           host='watershed.ucsd.edu',
                           db='cchdo')
  except MySQLdb.Error, e:
    print "Database error: %s" % e
    exit(1)

# Globals
WOCE_to_OceanSITES_flag = {
  1: 3, # Not calibrated -> Bad data that are potentially correctable (re-calibration)
  2: 1, # Acceptable measurement -> Good data
  3: 2, # Questionable measurement -> Probably good data
  4: 4, # Bad measurement -> Bad data
  5: 9, # Not reported -> Missing value
  6: 8, # Interpolated over >2 dbar interval -> Interpolated value
  7: 5, # Despiked -> Value changed
  9: 9  # Not sampled -> Missing value
}

# Functions
def uniqify(seq): # Credit: Dave Kirby
  seen = set()
  return [x for x in seq if x not in seen and not seen.add(x)]

def read_arbitrary(filename):
  if not exists(filename): raise ValueError("The file '%s' does not exist" % filename)
  if filename.endswith('zip'):
    datafile = DataFileCollection()
  else:
    datafile = DataFile()
  if filename.endswith('su.txt'):
    datafile.read_SUM_WOCE(handle)
  elif filename.endswith('hy.txt'):
    datafile.read_Bottle_WOCE(handle)
  elif filename.endswith('hy1.csv'):
    datafile.read_Bottle_Exchange(handle)
  elif filename.endswith('nc_hyd.zip'):
    datafile.read_BottleZip_NetCDF(handle)
  elif filename.endswith('ct.zip'):
    datafile.read_CTDZip_WOCE(handle)
  elif filename.endswith('ct1.zip'):
    datafile.read_CTDZip_Exchange(handle)
  elif filename.endswith('nc_ctd.zip'):
    datafile.read_CTDZip_NetCDF(handle)
  else:
    raise ValueError('Unrecognized file type for %s' % filename)
  return datafile

def woce_lat_to_dec_lat(self, lattoks):
  lat = int(lattoks[0]) + float(lattoks[1])/60.0
  if lattoks[2] is not 'N':
    lat *= -1
  return lat
def woce_lng_to_dec_lng(self, lngtoks):
  lng = int(lngtoks[0]) + float(lngtoks[1])/60.0
  if lngtoks[2] is not 'E':
    lng *= -1
  return lng
def dec_lat_to_woce_lat(self, lat):
  lat_deg = int(lat)
  lat_dec = abs(lat-lat_deg) * 60
  lat_deg = abs(lat_deg)
  lat_hem = 'S'
  if lat > 0:
    lat_hem = 'N'
  return '%2d %05.2f %1s' % (lat_deg, lat_dec, lat_hem)
def dec_lng_to_woce_lng(self, lng):
  lng_deg = int(lng)
  lng_dec = abs(lng-lng_deg) * 60
  lng_deg = abs(lng_deg)
  lng_hem = 'W'
  if lng > 0 :
    lng_hem = 'E'
  return '%3d %05.2f %1s' % (lng_deg, lng_dec, lng_hem)

PARTIAL_PRES_WATER = 2.184e-6 # TODO confirm this with Jim

def depth(grav, p, rho, depth):
  '''
  Calculate depth by integration of insitu density.
    grav - local gravity (m/sec^2) @ 0.0 db
    p - pressure series (decibars)
    rho - insitu density series (kg/m^3)
    depth - depth series (meters)
  '''
  num_intervals = len(p)
  if not (num_intervals is len(rho) is len(depth)):
    raise ValueError("The number of series intervals must be the same.")

  # When calling depth() repeatedly with a two-element
  # series, the first call should be with a one-element series to
  # initialize the starting value (see depth_(), below).

  # Initialize the series
  if num_intervals is not 2:
    # If the integration starts from > 15 db, calculate depth relative to
    # starting place. Otherwise, calculate from surface.
    if p[0] > 15.0:
      depth[0] = 0.0
    else:
      depth[0] = p[0]/(rho[0]*10000.0*(grav+PARTIAL_PRES_WATER*p[0]))
  # Calculate the rest of the series.
  for i in range(0, num_intervals-2):
    j = i+1
    # depth in meters
    depth[j] = depth[i] + (p[j]-p[i]) / ((rho[j]+rho[i])*5000.0*(grav+PARTIAL_PRES_WATER*p[j]))

def depth_unesco(pres, lat):
  '''
  Depth (meters) from pressure (decibars) using Saunders and Fofonoff's method.
  Deep-sea Res., 1976, 23, 109-111.
  Formula refitted for 1980 equation of state
  Ported from Unesco 1983
  Units:
    pressure  p     decibars
    latitude  lat   degrees
    depth     depth meters
  Checkvalue: depth = 9712.653 M for P=10000 decibars, latitude=30 deg above
    for standard ocean: T=0 deg celsius; S=35 (PSS-78)
  '''
  x = pow(sin(lat/57.29578), 2)
  gr = 9.780318*(1.0+(5.2788e-3 + 2.36e-5 * x) * x) + 1.092e-6 * pres
  return ((((-1.82e-15 * pres + 2.279e-10) * pres - 2.2512e-5)
           * pres + 9.72659) * pres) / gr

class Parameter:
  def __init__(self, parameter_name):
    connection = connect_postgresql()
    cursor = connection.cursor()
    select = ','.join(['parameters.name', 'format', 'description', 'units',
                       'bound_lower', 'bound_upper', 'units.mnemonic_woce',
                       'parameters_orders.order'])
    cursor.execute("SELECT "+select+"""
                    FROM parameters
                    INNER JOIN parameters_aliases ON parameters.id = parameters_aliases.parameter_id
                    LEFT JOIN parameters_orders ON parameters.id = parameters_orders.parameter_id
                    LEFT JOIN units ON parameters.units = units.id
                    WHERE parameters_aliases.name = %s
                    LIMIT 1""", (parameter_name,))
    row = cursor.fetchone()
    if row:
      self.full_name = row[0]
      self.format = row[1]
      self.description = row[2]
      self.units = row[3]
      self.bound_lower = row[4]
      self.bound_upper = row[5]
      self.units_mnemonic = row[6]
      self.woce_mnemonic = parameter_name
      self.display_order = row[7] or -9999
      self.aliases = []
    else:
      connection.close()
      raise NameError("'"+parameter_name+"' is not in CCHDO's parameter list.")
    connection.close()
  def __str__(self):
    return 'Parameter '+self.woce_mnemonic

class Column:
  def __init__(self, parameter):
    if isinstance(parameter, Parameter):
      self.parameter = parameter
    else:
      self.parameter = Parameter(parameter)
    self.values = []
    self.flags_woce = []
    self.flags_igoss = []
  def get(self, index):
    return self.values[index]
  def set(self, index, value, flag_woce=None, flag_igoss=None):
    while index > len(self.values):
      self.values.append(None)
      self.flags_woce.append(None)
      self.flags_igoss.append(None)
    self.values.insert(index, value)
    if flag_woce:
      self.flags_woce.insert(index, flag_woce)
    if flag_igoss:
      self.flags_igoss.insert(index, flag_igoss)
  def append(self, value=None, flag_woce=None, flag_igoss=None):
    if value:
      self.values.append(value)
    if flag_woce:
      self.flags_woce.append(flag_woce)
    if flag_igoss:
      self.flags_igoss.append(flag_igoss)
  def __getitem__(self, key):
    return self.get(key)
  def __setitem__(self, key, value):
    self.set(key, value)
  def __len__(self):
    return len(self.values)
  def is_flagged(self):
    return self.is_flagged_woce() or self.is_flagged_igoss()
  def is_flagged_woce(self):
    return not (self.flags_woce is None or len(self.flags_woce) == 0)
  def is_flagged_igoss(self):
    return not (self.flags_igoss is None or len(self.flags_igoss) == 0)
  def __str__(self):
    return 'Column of '+str(self.parameter)+':'+str(self.values)
  def __cmp__(self, other):
    return self.parameter.display_order - other.parameter.display_order

class SummaryColumn(Column):
  def __init__(self, parameter):
    self.parameter = parameter
    self.values = []

class SummaryFile:
  def __init__(self):
    self.columns = {}
    self.header = ''
    columns = ['EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO', 'DATE', 'TIME',
               'LATITUDE', 'LONGITUDE', 'DEPTH', '_CAST_TYPE', '_CODE',
               '_NAV', '_ABOVE_BOTTOM', '_MAX_PRESSURE', '_NUM_BOTTLES',
               '_PARAMETERS', '_COMMENTS']
    for column in columns:
      self.columns[column] = SummaryColumn(column)
  def __len__(self):
    if not self.columns.values():
      return 0
    return len(self.columns.values()[0])
  def read_HOT_Summary(self, handle):
    '''How to read a HOT Summary file.'''
    header = True
    header_delimiter = compile('^-+$')
    for line in handle:
      if header:
        if header_delimiter.match(line):
          header = False
        else:
          self.header += line
      else:
        if len(line) is 0: continue
        tokens = line.split()
        self.columns['EXPOCODE'].append(tokens[0].replace('/', '_'))
        self.columns['SECT_ID'].append(tokens[1])
        self.columns['STNNBR'].append(int(tokens[2]))
        self.columns['CASTNO'].append(int(tokens[3]))
        self.columns['_CAST_TYPE'].append(tokens[4])
        date = datetime.strptime(tokens[5], '%m%d%y')
        self.columns['DATE'].append('%4d%02d%02d' % (date.year, date.month, date.day))
        self.columns['TIME'].append(int(tokens[6]))
        self.columns['_CODE'].append(tokens[7])
        lat = woce_lat_to_dec_lat(tokens[8:11])
        self.columns['LATITUDE'].append(lat)
        lng = woce_lng_to_dec_lng(tokens[11:14])
        self.columns['LONGITUDE'].append(lng)
        self.columns['_NAV'].append(tokens[14])
        self.columns['DEPTH'].append(int(tokens[15]))
        self.columns['_ABOVE_BOTTOM'].append(int(tokens[16]))
        self.columns['_MAX_PRESSURE'].append(int(tokens[17]))
        self.columns['_NUM_BOTTLES'].append(int(tokens[18]))
        self.columns['_PARAMETERS'].append(tokens[19])
        self.columns['_COMMENTS'].append(' '.join(tokens[20:]))
  def read_WOCE_Summary(self, handle):
    pass # TODO
  def write_WOCE_Summary(self, handle):
    '''How to write a WOCE Summary file.'''
    today = date.today()
    uniq_sects = uniqify(self.columns['SECT_ID'].values)
    handle.write('R/V _SHIP LEG _# WHP-ID '+','.join(uniq_sects)+' %04d%02d%02d' % (today.year, today.month, today.day)+"SIOCCHDOLIB\n")
    header_one = 'SHIP/CRS       WOCE               CAST         UTC           POSITION                UNC   COR ABOVE  WIRE   MAX  NO. OF\n'
    header_two = 'EXPOCODE       SECT STNNBR CASTNO TYPE DATE   TIME CODE LATITUDE   LONGITUDE   NAV DEPTH DEPTH BOTTOM  OUT PRESS BOTTLES PARAMETERS      COMMENTS            \n'
    header_sep = ('-' * (len(header_two)-1)) + '\n'
    handle.write(header_one)
    handle.write(header_two)
    handle.write(header_sep)
    for i in range(0, len(self)):
      exdate = self.columns['DATE'][i]
      date_str = exdate[4:6]+exdate[6:8]+exdate[2:4]
      row = ('%-14s %-5s %5s    %3d  %3s %-6s %04d   %2s %-10s %-11s %3s %5d       %-6d      %5d %7d %-15s %-20s' %
        ( self.columns['EXPOCODE'][i], self.columns['SECT_ID'][i],
          self.columns['STNNBR'][i], self.columns['CASTNO'][i],
          self.columns['_CAST_TYPE'][i], date_str,
          self.columns['TIME'][i], self.columns['_CODE'][i],
          dec_lat_to_woce_lat(self.columns['LATITUDE'][i]),
          dec_lng_to_woce_lng(self.columns['LONGITUDE'][i]),
          self.columns['_NAV'][i], self.columns['DEPTH'][i],
          self.columns['_ABOVE_BOTTOM'][i], self.columns['_MAX_PRESSURE'][i],
          self.columns['_NUM_BOTTLES'][i], self.columns['_PARAMETERS'][i],
          self.columns['_COMMENTS'][i] ))
      handle.write(row+'\n')
  def write_nav(self, handle): # TODO consolidate with DataFile's write_nav? Same code.
    for i in range(0, len(self)):
      lat = self.columns['LATITUDE'][i]
      lng = self.columns['LONGITUDE'][i]
      handle.write('%3.3f %3.3f\n' % (lng, lat))

class DataFile:
  def __init__(self):
    self.columns = {}
    self.stamp = None
    self.header = ''
    self.footer = None
    self.globals = {}
  def expocodes(self):
    return uniqify(self.columns['EXPOCODE'])
  def __len__(self):
    if not self.columns.values():
      return 0
    return len(self.columns.values()[0])
  def sorted_columns(self):
    return sorted(self.columns.values())
  def get_property_for_columns(self, property_getter):
    return map(property_getter, self.sorted_columns())
  def column_headers(self):
    return self.get_column_property(lambda column: column.parameter.woce_mnemonic)
  def precisions(self):
    return self.get_column_property(lambda column: column.parameter.precision)
  def to_hash(self):
    hash = {}
    for column in self.columns:
      hash[column.parameter.woce_mnemonic] = column.values
    return hash

  # IO methods
  def read_db(self):
    pass
  def write_db(self):
    pass
  def write_track_lines(self):
    '''How to write a trackline entry to the MySQL database'''
    connection = connect_mysql()
    cursor = connection.cursor()
    expocodes = self.columns['EXPOCODE'].values
    for expocode in self.expocodes():
      indices = [i for i, x in enumerate(expocodes) if x == expocode]
      lngs = [self.columns['LONGITUDE'][i] for i in indices]
      lats = [self.columns['LATITUDE'][i] for i in indices]
      points = zip(lngs, lats)
      linestring = 'LINESTRING('+','.join(map(lambda p: ' '.join(p), points))+')'
      sql = 'SET @g = LineStringFromText("'+linestring+'"); INSERT IGNORE INTO track_lines VALUES(DEFAULT,"'+expocode+'",@g,"Default") ON DUPLICATE KEY UPDATE Track = @g'
      cursor.execute(sql)
    cursor.close()
    connection.close()
  def write_nav(self, handle):
    for i in range(0, len(self)):
      lat = self.columns['LATITUDE'][i]
      lng = self.columns['LONGITUDE'][i]
      handle.write('%3.3f %3.3f\n' % (lng, lat))
  def read_CTD_WOCE(self, handle):
    '''How to read a CTD WOCE file.'''
    pass # TODO
  def write_CTD_WOCE(self, handle):
    '''How to write a CTD WOCE file.'''
    pass # TODO
  def read_CTD_Exchange(self, handle):
    '''How to read a CTD Exchange file.'''
    # Read identifier and stamp
    stamp = compile('CTD,(\d{8}\w+)')
    m = stamp.match(handle.readline())
    if m:
      self.stamp = m.group(1)
    else:
      raise ValueError('Expected identifier line with stamp (e.g. #BOTTLE,YYYYMMDDdivINSwho)')
    # Read comments
    l = handle.readline()
    while l and l.startswith('#'):
      self.header += l
      l = handle.readline()
    # Read NUMBER_HEADERS
    num_headers = compile('NUMBER_HEADERS\s+=\s+(\d+)')
    m = num_headers.match(handle.readline())
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

    # Check for unique identifer
    identifier = []
    global_keys = self.globals.keys()
    if 'EXPOCODE' in global_keys and 'STNNBR' in global_keys and 'CASTNO' in global_keys:
      identifier = ['STNNBR', 'CASTNO']
      if 'SAMPNO' in global_keys:
        identifier.append('SAMPNO')
        if 'BTLNBR' in global_keys:
          identifier.append('BTLNBR')
      elif 'BTLNBR' in global_keys:
        identifier.append('BTLNBR')
      else:
        raise ValueError('No unique identifer found for file. (STNNBR,CASTNO,SAMPNO,BTLNBR),(STNNBR,CASTNO,SAMPNO),(STNNBR,CASTNO,BTLNBR)')

    # Create internal columns and check units
    for column, unit in zip(columns, units):
      if column.endswith('FLAG_W') or column.endswith('FLAG_I'): continue
      self.columns[column] = Column(column)
      if self.columns[column].parameter.units != unit:
        pass # TODO warn about mismatched units line with CCHDO units or do conversion

    # Read data
    l = handle.readline().strip()
    while l:
      if l == 'END_DATA': break
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
  def write_CTD_Exchange(self, handle):
    '''How to write a CTD Exchange file.'''
    today = date.today()
    handle.write('CTD,'+str(today.year)+str(today.month)+str(today.day)+"SIOCCHDOLIB\n")
    handle.write('NUMBER_HEADERS = '+str(len(self.globals.keys())+1)+"\n")
    for key in self.globals.keys():
      handle.write(key+' = '+str(self.globals[key])+"\n")
    handle.write(','.join(self.column_headers())+"\n")
    for i in range(len(self)):
      handle.write(','.join(map(lambda header: str(self.columns[header][i]),
                                 self.column_headers()))+"\n")
    handle.write("END_DATA\n")
  def read_CTD_NetCDF(self, handle):
    '''How to read a CTD NetCDF file.'''
    filename = handle.name
    handle.close()
    nc_file = Dataset(filename, 'r')
    # Create columns for all the variables and get all the data.
    nc_ctd_var_to_woce_param = {'cast': 'CASTNO',
                                'temperature': 'CTDTMP',
                                'time': 'drop',
                                'woce_date': 'DATE',
                                'oxygen': 'CTDOXY',
                                'salinity': 'CTDSAL',
                                'pressure': 'CTDPRS',
                                'station': 'STNNBR',
                                'longitude': 'LONGITUDE',
                                'latitude': 'LATITUDE',
                                'woce_time': 'TIME',
                               }
    qc_vars = {}
    # First pass to create columns
    for name, variable in nc_file.variables.items():
      if name.endswith('_QC'):
        qc_vars[nc_ctd_var_to_woce_param[name[:-3]]] = variable
      else:
        name = nc_ctd_var_to_woce_param[name]
        if name is 'drop':
          continue
        self.columns[name] = Column(name)
        self.columns[name].values = variable[:].tolist()
        if name == 'STNNBR' or name == 'CASTNO':
          self.columns[name].values = [''.join(self.columns[name].values)]
        if len(self.columns[name].values) <= 1 or not self.columns[name].values[1]:
          self.globals[name] = self.columns[name].get(0)
          del self.columns[name]
    # Second pass to put in flags
    for name, variable in qc_vars.items():
      self.columns[name].flags_woce = variable[:].tolist()
    # Rename globals to CCHDO recognized ones
    global_attrs = nc_file.__dict__
    globals_to_rename_as = {'CAST_NUMBER': 'CASTNO',
                            'STATION_NUMBER': 'STNNBR',
                            'BOTTOM_DEPTH_METERS': 'DEPTH',
                            'WOCE_ID': 'SECT_ID',
                            'EXPOCODE': 'EXPOCODE',
                           }
    for g, param in globals_to_rename_as.items():
      self.globals[param] = str(global_attrs[g])
    # Get stamp
    self.stamp = global_attrs['ORIGINAL_HEADER']
    # Clean up
    nc_file.close()
  def write_CTD_NetCDF(self, handle):
    '''How to write a CTD NetCDF file.'''
    pass # TODO
  def read_CTD_NetCDF_OceanSITES(self, handle):
    '''How to read a CTD NetCDF OceanSITES file.'''
    pass # TODO
  def write_CTD_NetCDF_OceanSITES(self, handle):
    '''How to write a CTD NetCDF OceanSITES file.'''
    filename = handle.name
    handle.close()
    Infinity = 1e10000
    NaN = Infinity/Infinity
    strdate = str(self.globals['DATE']) 
    strtime = str(self.globals['TIME']).rjust(4, '0')
    isowocedate = datetime(int(strdate[0:4]), int(strdate[5:6]), int(strdate[7:8]),
		                       int(strtime[0:2]), int(strtime[3:5]))

    nc_file = Dataset(filename, 'w', format='NETCDF3_CLASSIC')
    nc_file.data_type = 'OceanSITES time-series CTD data'
    nc_file.format_version = '1.1'
    nc_file.date_update = datetime.utcnow().isoformat()+'Z'
    nc_file.wmo_platform_code = ''
    nc_file.source = 'Shipborne observation'
    nc_file.history = isowocedate.isoformat()+"Z data collected\n"+datetime.utcnow().isoformat()+"Z date file translated/written"
    nc_file.data_mode = 'D'
    nc_file.quality_control_indicator = '1'
    nc_file.quality_index = 'B'
    nc_file.conventions = 'OceanSITES Manual 1.1, CF-1.1'
    nc_file.netcdf_version = '3.x'
    nc_file.naming_authority = 'OceanSITES'
    nc_file.cdm_data_type = 'Station'
    nc_file.geospatial_lat_min = str(self.globals['LATITUDE'])
    nc_file.geospatial_lat_max = str(self.globals['LATITUDE'])
    nc_file.geospatial_lon_min = str(self.globals['LONGITUDE'])
    nc_file.geospatial_lon_max = str(self.globals['LONGITUDE'])
    nc_file.geospatial_vertical_min = int(self.globals['DEPTH'])
    nc_file.geospatial_vertical_max = 0
    nc_file.author = 'Shen:Diggs (Scripps)'
    nc_file.data_assembly_center = 'SIO'
    nc_file.distribution_statement = 'Follows CLIVAR (Climate Varibility and Predictability) standards, cf. http://www.clivar.org/data/data_policy.php. Data available free of charge. User assumes all risk for use of data. User must display citation in any publication or product using data. User must contact PI prior to any commercial use of data.'
    nc_file.citation = 'These data were collected and made freely available by the OceanSITES project and the national programs that contribute to it.'
    nc_file.update_interval = 'void'
    nc_file.qc_manual = "OceanSITES User's Manual v1.1"
    nc_file.time_coverage_start = isowocedate.isoformat()+'Z'
    nc_file.time_coverage_end = isowocedate.isoformat()+'Z'

    nc_file.createDimension('TIME')
    nc_file.createDimension('PRES', len(self))
    nc_file.createDimension('LATITUDE', 1)
    nc_file.createDimension('LONGITUDE', 1)
    nc_file.createDimension('POSITION', 1)

    # OceanSITES coordinate variables
    var_time = nc_file.createVariable('TIME', 'd', ('TIME',))
    var_time.long_name = 'time'
    var_time.standard_name = 'time'
    var_time.units = 'days since 1950-01-01T00:00:00Z'
    var_time._FillValue = 999999.0
    var_time.valid_min = 0.0
    var_time.valid_max = 90000.0
    var_time.QC_indicator = 7 # Matthias Lankhorst
    var_time.QC_procedure = 5 # Matthias Lankhorst
    var_time.uncertainty = 0.0417 # 1/24 assuming a typical cast lasts one hour Matthias Lankhorst
    var_time.axis = 'T'

    var_latitude = nc_file.createVariable('LATITUDE', 'f', ('LATITUDE',))
    var_latitude.long_name = 'Latitude of each location'
    var_latitude.standard_name = 'latitude'
    var_latitude.units = 'degrees_north'
    var_latitude._FillValue = 99999.0
    var_latitude.valid_min = -90.0
    var_latitude.valid_max = 90.0
    var_time.QC_indicator = 7 # Matthias Lankhorst
    var_time.QC_procedure = 5 # Matthias Lankhorst
    var_latitude.uncertainty = 0.0045 # Matthias Lankhorst
    var_latitude.axis = 'Y'

    var_longitude = nc_file.createVariable('LONGITUDE', 'f', ('LONGITUDE',))
    var_longitude.long_name = 'Longitude of each location'
    var_longitude.standard_name = 'longitude'
    var_longitude.units = 'degrees_east'
    var_longitude._FillValue = 99999.0
    var_longitude.valid_min = -180.0
    var_longitude.valid_max = 180.0
    var_time.QC_indicator = 7 # Matthias Lankhorst
    var_time.QC_procedure = 5 # Matthias Lankhorst
    var_longitude.uncertainty = 0.0045/cos(self.globals['LATITUDE']) # Matthias Lankhorst
    var_longitude.axis = 'X'

    var_pressure = nc_file.createVariable('PRES', 'f', ('PRES',))
    var_pressure.long_name = 'sea water pressure'
    var_pressure.standard_name = 'sea_water_pressure'
    var_pressure.units = 'decibar'
    var_pressure._FillValue = NaN
    var_pressure.valid_min = 0.0
    var_pressure.valid_max = 12000.0
    var_pressure.QC_indicator = 7 # Matthias Lankhorst
    var_pressure.QC_procedure = 5 # Matthias Lankhorst
    var_pressure.uncertainty = 2.0
    var_pressure.axis = 'Z'

    since_1950 = isowocedate - datetime(1950, 1, 1)
    var_time[:] = [since_1950.days + since_1950.seconds/86400.0]
    var_latitude[:] = [self.globals['LATITUDE']]
    var_longitude[:] = [self.globals['LONGITUDE']]

    # CTD variables
    param_to_oceansites = {
      'ctd_pressure': 'PRES',
      'ctd_temperature': 'TEMP',
      'ctd_oxygen': 'DOXY',
      'ctd_salinity': 'PSAL'
    }
    oceansites_variables = {
      'TEMP': {'long': 'sea water temperature', 'std': 'sea_water_temperature', 'units': 'degree_Celsius'},
      'DOXY': {'long': 'dissolved oxygen', 'std': 'dissolved_oxygen', 'units': 'micromole/kg'},
      'PSAL': {'long': 'sea water salinity', 'std': 'sea_water_salinity', 'units': 'psu'}
    }
    oceansites_uncertainty = {
      'TEMP': 0.002,
      'PSAL': 0.005,
      'DOXY': Infinity,
      'PRES': Infinity
    }

    for column in self.columns.values():
      name = column.parameter.description.lower().replace(' ', '_').replace('(', '_').replace(')', '_')
      if name in param_to_oceansites.keys():
        name = param_to_oceansites[name]
      # Write variable
      if name is not 'PRES':
        var = nc_file.createVariable(name, 'f8', ('PRES',))
        variable = oceansites_variables[name]
        var.long_name = variable['long'] or ''
        var.standard_name = variable['std'] or ''
        var.units = variable['units'] or ''
        var._FillValue = NaN # TODO ref table 3
        var.QC_procedure = 5 # Data manually reviewed
        var.QC_indicator = 2 # Probably good data
        var.valid_min = column.parameter.bound_lower
        var.valid_max = column.parameter.bound_upper
        var.sensor_depth = -999 # TODO nominal sensor depth in meters positive in direction of DEPTH:positive
        var.cell_methods = 'TIME: point DEPTH: average LATITUDE: point LONGITUDE: point'
        var.uncertainty = oceansites_uncertainty[name]
        var[:] = column.values
      else:
        var_pressure[:] = column.values
      # Write QC variable
      if column.is_flagged_woce():
        var.ancillary_variables = name+'_QC'
        flag = nc_file.createVariable(name+'_QC', 'b', ('PRES',))
        flag.long_name = 'quality flag'
        flag.conventions = 'OceanSITES reference table 2'
        flag._FillValue = -128
        flag.valid_min = 0
        flag.valid_max = 9
        flag.flag_values = 0#, 1, 2, 3, 4, 5, 6, 7, 8, 9 TODO??
        flag.flag_meanings = ' '.join(['no_qc_performed',
                                       'good_data',
                                       'probably_good_data',
                                       'bad_data_that_are_potentially_correctable',
                                       'bad_data',
                                       'value_changed',
                                       'not_used',
                                       'nominal_value',
                                       'interpolated_value',
                                       'missing_value'
                                      ])
        flag[:] = map(lambda x: WOCE_to_OceanSITES_flag[x], column.flags_woce)
    nc_file.close()
  def write_CTD_NetCDF_OceanSITES_BATS(self, handle):
    '''How to write a CTD NetCDF OceanSITES BATS file.'''
    self.write_CTD_NetCDF_OceanSITES(handle)
    filename = handle.name
    nc_file = Dataset(filename, 'a', format='NETCDF3_CLASSIC')
    nc_file.title = ' '.join(['BATS CTD Timeseries', 'ExpoCode='+self.globals['EXPOCODE'],
                              'Station='+self.globals['STNNBR'], 'Cast='+self.globals['CASTNO']])
    strdate = str(self.globals['DATE']) 
    isodate = datetime(int(strdate[0:4]), int(strdate[5:6]), int(strdate[7:8]), 0, 0)
    stringdate = isodate.date().isoformat().replace('-', '')
    nc_file.platform_code = 'BATS'
    nc_file.institution = 'Bermuda Institute of Ocean Sciences'
    nc_file.institution_references = 'http://bats.bios.edu/'
    nc_file.site_code = 'BIOS-BATS'
    nc_file.references = 'http://cchdo.ucsd.edu/search?query=group:BATS'
    nc_file.comment = 'BIOS-BATS CTD data from SIO, translated to OceanSITES NetCDF by SIO'
    nc_file.summary = 'BIOS-BATS CTD data Bermuda'
    nc_file.area = 'Atlantic - Sargasso Sea'
    nc_file.institution_references = 'http://bats.bios.edu/'
    nc_file.contact = 'rodney.johnson@bios.edu'
    nc_file.pi_name = 'Rodney Johnson'
    nc_file.id = '_'.join(['OS', 'BERMUDA', stringdate, 'SOT'])
    nc_file.close()
  def write_CTD_NetCDF_OceanSITES_HOT(self, handle):
    '''How to write a CTD NetCDF OceanSITES HOT file.'''
    self.write_CTD_NetCDF_OceanSITES(handle)
    filename = handle.name
    nc_file = Dataset(filename, 'a', format='NETCDF3_CLASSIC')
    nc_file.title = ' '.join(['HOT CTD Timeseries', 'ExpoCode='+self.globals['EXPOCODE'],
                              'Station='+self.globals['STNNBR'], 'Cast='+self.globals['CASTNO']])
    strdate = str(self.globals['DATE']) 
    isodate = datetime(int(strdate[0:4]), int(strdate[5:6]), int(strdate[7:8]), 0, 0)
    stringdate = isodate.date().isoformat().replace('-', '')
    nc_file.platform_code = 'HOT'
    nc_file.institution = "University of Hawai'i School of Ocean and Earth Science and Technology"
    nc_file.site_code = 'ALOHA'
    nc_file.references = 'http://cchdo.ucsd.edu/search?query=group:HOT'
    nc_file.comment = 'HOT CTD data from SIO, translated to OceanSITES NetCDF by SIO'
    nc_file.summary = "HOT CTD data Hawai'i"
    nc_file.area = "Pacific - Hawai'i"
    nc_file.institution_references = 'http://hahana.soest.hawaii.edu/hot/hot_jgofs.html'
    nc_file.contact = 'santiago@soest.hawaii.edu'
    nc_file.pi_name = 'Roger Lukas'
    nc_file.id = '_'.join(['OS', 'ALOHA', stringdate, 'SOT'])
    nc_file.close()
  def read_CTD_ODEN(self, handle):
    '''How to read a CTD ODEN file.'''
    lineno = 1
    for line in handle:
      if lineno == 1:
        self.globals['SECT_ID'] = line[3:5] # Leg
        self.globals['STNNBR'] = line[7:10] # Station
        self.globals['CASTNO'] = line[13:15] # Cast
        self.globals['DATE'] = '19'+line[19:21]+line[17:19]+line[15:17]
        self.globals['TIME'] = line[41:45] # GMT Time(hhmm)
        #self.globals['cast_type'] = line[23:26]
        lat_deg = int(line[26:28])
        lat_min = float(line[28:32])
        lat_hem = line[32]
        if lat_hem == 'N':
          self.globals['LATITUDE'] = str(lat_deg+lat_min/60)
        elif lat_hem == 'S':
          self.globals['LATITUDE'] = str(-(lat_deg+lat_min/60))

        lng_deg = int(line[34:36])
        lng_min = float(line[36:40])
        lng_hem = line[40]
        if lng_hem == 'E':
          self.globals['LONGITUDE'] = str(lng_deg+lng_min/60)
        elif lng_hem == 'W':
          self.globals['LONGITUDE'] = str(-(lng_deg+lng_min/60))

        self.globals['DEPTH'] = line[45:50] # PDR Bottom Depth
        #self.globals['remarks'] = line[50:-1]

        self.columns['CTDPRS'] = Column('CTDPRS')
        self.columns['CTDTMP'] = Column('CTDTMP')
        self.columns['CTDCND'] = Column('CTDCND')
        self.columns['CTDSAL'] = Column('CTDSAL')
        self.columns['POTTMP'] = Column('POTTMP')
      else:
        data = line.split()
        row = lineno-2
        self.columns['CTDPRS'][row] = data[0]
        self.columns['CTDTMP'][row] = data[1] # need conversion from ITPS-68 to ITS-90?
        self.columns['CTDCND'][row] = data[2]
        self.columns['CTDSAL'][row] = data[3]
        self.columns['POTTMP'][row] = data[4]
      lineno += 1
  def write_CTD_ODEN(self,handle):
    pass # OMIT
  def read_Bottle_WOCE(self, handle):
    '''How to read a Bottle WOCE file.'''
    pass # TODO
  def write_Bottle_WOCE(self, handle):
    '''How to write a Bottle WOCE file.'''
    pass # TODO
  def read_Bottle_Exchange(self, handle):
    '''How to read a Bottle Exchange file.'''
    # Read identifier and stamp
    stamp = compile('BOTTLE,(\d{8}\w+)')
    m = stamp.match(handle.readline())
    if m:
      self.stamp = m.group(1)
    else:
      raise ValueError("Expected identifier line with stamp (e.g. #BOTTLE,YYYYMMDDdivINSwho)")
    # Read comments
    l = handle.readline()
    while l and l.startswith('#'):
      self.header += l
      l = handle.readline()
    # Read columns and units
    columns = l.strip().split(',')
    units = handle.readline().strip().split(',')
    
    # Check columns and units to match length
    if len(columns) is not len(units):
      raise ValueError("Expected as many columns as units in file. Found %d columns and %d units." % (len(columns), len(units)))

    # Check for unique identifer
    identifier = []
    if 'EXPOCODE' in columns and 'STNNBR' in columns and 'CASTNO' in columns:
      identifier = ['STNNBR', 'CASTNO']
      if 'SAMPNO' in columns:
        identifier.append('SAMPNO')
        if 'BTLNBR' in columns:
          identifier.append('BTLNBR')
      elif 'BTLNBR' in columns:
        identifier.append('BTLNBR')
      else:
        raise ValueError('No unique identifer found for file. (STNNBR,CASTNO,SAMPNO,BTLNBR),(STNNBR,CASTNO,SAMPNO),(STNNBR,CASTNO,BTLNBR)')

    # Create internal columns and check units
    for column, unit in zip(columns, units):
      if column.endswith('FLAG_W') or column.endswith('FLAG_I'): continue
      self.columns[column] = Column(column)
      if self.columns[column].parameter.units != unit:
        pass # TODO warn about mismatched units line with CCHDO units or do conversion

    # Read data
    l = handle.readline().strip()
    while l:
      if l == 'END_DATA': break
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
  def write_Bottle_Exchange(self, handle):
    '''How to write a Bottle Exchange file.'''
    pass
  def read_Bottle_NetCDF(self, handle):
    '''How to read a Bottle NetCDF file.'''
    pass
  def write_Bottle_NetCDF(self, handle):
    '''How to write a Bottle NetCDF file.'''
    # This time, the handle is actually a path to a tempdir to give to the
    # NetCDF library to write in.
    expocode = self.columns['EXPOCODE'][0]
    station = self.columns['STNNBR'][0].rjust(5, '0')
    cast = self.columns['CASTNO'][0].rjust(5, '0')
    filename = '_'.join(expocode, station, cast, 'hy1')+'.nc'
    fullpath = handle+'/'+filename

    nc_file = NetCDFFile(fullpath, 'w')

    # Write dimension variables
    dim_bottle = nc_file.createDimension('bottle', len(self))
    dim_time = nc_file.createDimension('time', 1)
    dim_lat = nc_file.createDimension('latitude', 1)
    dim_lng = nc_file.createDimension('longitude', 1)
    dims_variable = (dim_bottle, dim_time, dim_lat, dim_lng)
    dims_static = (dim_time, dim_lat, dim_lng)

    dim_string = nc_file.createDimension('string_dimension', 10)
    dims_string = (dim_string, dim_time)

    # Sometimes, there's no WOCE Section associated with a certain STNNBR and
    # CASTNO. In that case, let the user known it's an UNKNOWN section
    sect = self.columns['SECT_ID'][0]
    if sect is '':
      sect = 'UNKNOWN'
    setattr(nc_file, 'EXPOCODE', expocode)
    setattr(nc_file, 'Conventions', 'COARDS/WOCE')
    setattr(nc_file, 'WOCE_VERSION', '3.0')
    setattr(nc_file, 'WOCE_ID', sect)
    setattr(nc_file, 'DATA_TYPE', 'Bottle')
    setattr(nc_file, 'STATION_NUMBER', station)
    setattr(nc_file, 'CAST_NUMBER', cast)
    setattr(nc_file, 'BOTTOM_DEPTH_METERS', max(self.columns['DEPTH'].values))
    setattr(nc_file, 'BOTTLE_NUMBERS', ' '.join(self.columns['BTLNBR'].values))
    if self.columns['BTLNBR'].is_flagged_woce():
      setattr(nc_file, 'BOTTLE_QUALITY_CODES', ' '.join(self.columns['BTLNBR'].flags_woce))
    now = date(1970, 1, 1).now()
    setattr(nc_file, 'Creation_Time', str(now))
    header_filter = compile('BOTTLE|db_to_exbot|jjward|(Previous stamp)')
    header = '# Previous stamp: '+self.stamp+"\n"+"\n".join(filter(lambda x: not header_filter.match(x), self.header.split("\n")))
    setattr(nc_file, 'ORIGINAL_HEADER', header)
    setattr(nc_file, 'WOCE_BOTTLE_FLAG_DESCRIPTION', 
      ':'.join([
      ':',
      '1 = Bottle information unavailable.',
      '2 = No problems noted.',
      '3 = Leaking.',
      '4 = Did not trip correctly.',
      '5 = Not reported.',
      '6 = Significant discrepancy in measured values between Gerard and Niskin bottles.',
      '7 = Unknown problem.',
      '8 = Pair did not trip correctly. Note that the Niskin bottle can trip at an unplanned depth while the Gerard trips correctly and vice versa.',
      '9 = Samples not drawn from this bottle.',
      "\n"]))
    setattr(nc_file, 'WOCE_WATER_SAMPLE_FLAG_DESCRIPTION', 
      ':'.join([
      ':',
      '1 = Sample for this measurement was drawn from water bottle but analysis not received.', 
      '2 = Acceptable measurement.',
      '3 = Questionable measurement.',
      '4 = Bad measurement.',
      '5 = Not reported.',
      '6 = Mean of replicate measurements.',
      '7 = Manual chromatographic peak measurement.',
      '8 = Irregular digital chromatographic peak integration.',
      '9 = Sample not drawn for this measurement from this bottle.',
      "\n"]))
    ncvar = {}
    ncflagvar = {}
    for param, column in iter(self.columns):
      parameter = column.parameter
      parameter_name = parameter.mnemonic
      # continue if STATIC_PARAMETERS_PER_CAST. include parameter_name
      # TODO
    var_time = nc_file.createVariable('time', 'f', dims_static)
    setattr(var_time, 'long_name', 'time')
    setattr(var_time, 'units', 'minutes since 1980-01-01 00:00:00')
    setattr(var_time, 'data_min', 0)
    setattr(var_time, 'data_max', 0)
    setattr(var_time, 'C_format', '%10d')

    var_latitude = nc_file.createVariable('latitude', 'f', dims_static)
    setattr(var_latitude, 'long_name', 'latitude')
    setattr(var_latitude, 'units', 'degrees_N')
    setattr(var_latitude, 'data_min', 0)
    setattr(var_latitude, 'data_max', 0)
    setattr(var_latitude, 'C_format', '%9.4f')

    var_longitude = nc_file.createVariable('longitude', 'f', dims_static)
    setattr(var_longitude, 'long_name', 'longitude')
    setattr(var_longitude, 'units', 'degrees_E')
    setattr(var_longitude, 'data_min', 0)
    setattr(var_longitude, 'data_max', 0)
    setattr(var_longitude, 'C_format', '%9.4f')

    var_woce_date = nc_file.createVariable('woce_date', 'i', dims_static)
    setattr(var_woce_date, 'long_name', 'WOCE date')
    setattr(var_woce_date, 'units', 'yyyymdd UTC')
    setattr(var_woce_date, 'data_min', 0)#long min
    setattr(var_woce_date, 'data_max', 0)#long max
    setattr(var_woce_date, 'C_format', '%8d')
    
    var_woce_time = nc_file.createVariable('woce_time', 'i', dims_static)
    setattr(var_woce_time, 'long_name', 'WOCE time')
    setattr(var_woce_time, 'units', 'hhmm UTC')
    setattr(var_woce_time, 'data_min', 0)#long min
    setattr(var_woce_time, 'data_max', 0)#long max
    setattr(var_woce_time, 'C_format', '%4d')
    
    # Hydrographic specific
    
    var_station = nc_file.createVariable('station', 'c', dims_string)
    setattr(var_station, 'long_name', 'STATION')
    setattr(var_station, 'units', 'unspecified')
    setattr(var_station, 'C_format', '%s')
    
    var_cast = nc_file.createVariable('cast', 'c', dims_string)
    setattr(var_cast, 'long_name', 'CAST')
    setattr(var_cast, 'units', 'unspecified')
    setattr(var_cast, 'C_format', '%s')

    # Write out pairs TODO

    datetime = self.columns['DATE'][0]+self.columns['TIME']
    time_from_epoch = datetime # TODO
    cchdo_epoch_offset = datetime.date(1980, 01, 01)
    var_time[:] = (time_from_epoch - cchdo_epoch_offset)

    nc_file.close()

class DataFileCollection:
  def __init__(self):
    self.files = []
  def merge(datafile):
    pass
  def split(self):
    pass
  def stamps(self):
    return map(lambda file: file.stamp, self.files.values())
  # IO methods
  def read_CTDZip_WOCE(self, handle):
    '''How to read CTD WOCE files from a Zip.'''
    zip = ZipFile(handle, 'r')
    for file in zip.namelist():
      if 'README' in file or 'DOC' in file: continue
      tempstream = StringIO(zip.read(file))
      ctdfile = DataFile()
      ctdfile.read_CTD_WOCE(tempstream)
      self.files.append(ctdfile)
      tempstream.close()
    zip.close()
  def write_CTDZip_WOCE(self, handle):
    '''How to write CTD WOCE files to a Zip.'''
    pass # TODO
  def read_CTDZip_Exchange(self, handle):
    '''How to read CTD Exchange files from a Zip.'''
    zip = ZipFile(handle, 'r')
    for file in zip.namelist():
      if '.csv' not in file: continue
      tempstream = StringIO(zip.read(file))
      ctdfile = DataFile()
      ctdfile.read_CTD_Exchange(tempstream)
      self.files.append(ctdfile)
      tempstream.close()
    zip.close()
  def write_CTDZip_Exchange(self, handle):
    '''How to write CTD Exchange files to a Zip.'''
    zip = ZipFile(handle, 'w')
    for file in self.files:
      tempstream = StringIO()
      file.write_CTD_Exchange(tempstream)
      info = ZipInfo(file.globals['STNNBR'].strip()+'_'+file.globals['CASTNO'].strip()+'_ct1.csv')
      dt = datetime.now()
      info.date_time = (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
      info.external_attr = 0644 << 16L
      zip.writestr(info, tempstream.getvalue())
      tempstream.close()
    zip.close()
  def read_CTDZip_ODEN(self, handle):
    '''How to read CTD ODEN files from a Zip.'''
    zip = ZipFile(handle, 'r')
    for file in zip.namelist():
      if 'DOC' in file or 'README' in file:
        continue
      tempstream = StringIO(zip.read(file))
      ctdfile = DataFile()
      ctdfile.read_CTD_ODEN(tempstream)
      self.files.append(ctdfile)
      tempstream.close()
    zip.close()
  def write_CTDZip_ODEN(self, handle):
    '''How to write CTD ODEN files to a Zip.'''
    pass # OMIT
  # WOCE does not specify a BottleZip.
  # WHP-Exchange does not specify a BottleZip.
  def read_BottleZip_NetCDF(self, handle):
    '''How to read Bottle NetCDF files from a Zip.'''
    zip = ZipFile(handle, 'r')
    for file in zip.namelist():
      if '.csv' not in file: continue
      tempstream = StringIO(zip.read(file))
      ctdfile = DataFile()
      ctdfile.read_Bottle_NetCDF(tempstream)
      self.files.append(ctdfile)
      tempstream.close()
    zip.close()
  def write_BottleZip_NetCDF(self, handle):
    '''How to write Bottle NetCDF files to a Zip.'''
    # NetCDF libraries like to write to a file. Work around by giving temp dir.
    tempdir = mkdtemp()

    # The collection should already be split apart based on station cast.

    for file in self.files:
      file.write_Bottle_NetCDF(tempdir)

    zip = ZipFile(handle, 'w')
    for file in listdir(tempdir):
      fullpath = tempdir+'/'+file
      zip.write(fullpath)
      remove(fullpath)
    rmdir(tempdir)
    zip.close()

# TODO Regions...maybe break this out into different parts of the library?

class Location:
  def __init__(self, coordinate, datetime=None, depth=None):
    self.coordinate = coordinate
    self.datetime = datetime
    self.depth = depth
    # TODO nil axis magnitudes should be matched as a wildcard

class Region:
  def __init__(self, name, *locations):
    self.name = name
    self.locations = locations
  def include (location):
    raise NotImplementedError

BASINS = REGIONS = {
  'Pacific': Region('Pacific', Location([1.111, 2.222]), Location([-1.111, -2.222])),
  'East_Pacific': Region('East Pacific', Location([0, 0]), Location([1, 1]), Location([3, 3]))
  # TODO define the rest of the basins...maybe define bounds for other groupings
}

