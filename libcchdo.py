# libcchdo Python
#
# Dependencies
# ------------
# netcdf4-python - http://code.google.com/p/netcdf4-python/
#   netcdf
#   numpy - http://numpy.scipy.org/
# pygresql - http://www.pygresql.org/
#   postgresql server binaries
# MySQLdb - http://sourceforge.net/projects/mysql-python
#   MySQL server binaries
#
# Internal Data Specification
# ---------------------------
# Any unreported values must be represented as None. This includes -9,
# -999.000, unspecified dates, times, etc.
#
# Known unknown parameters have mnemonics that start with '_'. e.g. MAX
# PRESSURE exists in certain files but there is no parameter defined for it. By
# prefixing MAX_PRESSURE with a '_', the library will not retrive the parameter
# definition from the database (there is none anyway).
# 

from __future__ import with_statement
try:
  from netCDF3 import Dataset
except ImportError, e:
  raise ImportError('%s\n%s' % (e, 'You should get netcdf4-python from http://code.google.com/p/netcdf4-python and install the NetCDF 3 module as directed by the README.'))
try:
  import pgdb
except ImportError, e:
  raise ImportError('%s\n%s' % (e, 'You should get pygresql from http://www.pygresql.org/readme.html#where-to-get. You will need Postgresql with server binaries installed already.'))
try:
  import MySQLdb
except ImportError, e:
  raise ImportError('%s\n%s' % (e, 'You should get MySQLdb from http://sourceforge.net/projects/mysql-python. You will need MySQL with server binaries installed already.'))
from datetime import date, datetime
from numpy import dtype
from math import sin, cos, acos, pow, pi
from os import listdir, remove, rmdir
from os.path import exists
from re import compile, findall, finditer
from struct import unpack
from StringIO import StringIO
from tempfile import mkdtemp
from types import FloatType
from warnings import warn
from zipfile import ZipFile, ZipInfo
try:
  from math import isnan
except ImportError:
  def isnan(n):
    return n != n

# Globals
Infinity = 1e10000
NaN = Infinity/Infinity
RADIUS_EARTH = 6371.01 #km
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
def uniquify(seq): # Credit: Dave Kirby
  seen = set()
  return [x for x in seq if x not in seen and not seen.add(x)]
def strip_all(list):
  return map(lambda x: x.strip(), list)

def connect_postgresql():
  try:
    return pgdb.connect(user='libcchdo',
                        password='((hd0hydr0d@t@',
                        host='goship.ucsd.edu',
                        database='cchdotest')
  except pgdb.Error, e:
    raise IOError("Database error: %s" % e)
def connect_mysql():
  try:
    return MySQLdb.connect(user='jfields',
                           passwd='c@keandc00kies',
                           host='watershed.ucsd.edu',
                           db='cchdo')
  except MySQLdb.Error, e:
    raise IOError("Database error: %s" % e)

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

def great_circle_distance(lat_stand, lng_stand, lat_fore, lng_fore):
  delta_lng = lng_fore - lng_stand
  cos_lat_fore = cos(lat_fore)
  cos_lat_stand = cos(lat_stand)
  cos_lat_fore_cos_delta_lng = cos_lat_fore * cos(delta_lng)
  sin_lat_stand = sin(lat_stand)
  sin_lat_fore = sin(lat_fore)

  # Vicenty formula from Wikipedia
  # fraction_top = sqrt( (cos_lat_fore * sin(delta_lng)) ** 2 +
  #                      (cos_lat_stand * sin_lat_fore -
  #                       sin_lat_stand * cos_lat_fore_cos_delta_lng) ** 2)
  # fraction_bottom = sin_lat_stand * sin_lat_fore + cos_lat_stand * cos_lat_fore_cos_delta_lng
  # central_angle = atan2(1.0, fraction_top/fraction_bottom)

  # simple formula from wikipedia
  central_angle = acos(cos_lat_stand * cos_lat_fore * cos(delta_lng) +
                       sin_lat_stand * sin_lat_fore)

  arc_length = RADIUS_EARTH * central_angle
  return arc_length

def woce_lat_to_dec_lat(lattoks):
  lat = int(lattoks[0]) + float(lattoks[1])/60.0
  if lattoks[2] is not 'N':
    lat *= -1
  return lat
def woce_lng_to_dec_lng(lngtoks):
  lng = int(lngtoks[0]) + float(lngtoks[1])/60.0
  if lngtoks[2] is not 'E':
    lng *= -1
  return lng
def dec_lat_to_woce_lat(lat):
  lat_deg = int(lat)
  lat_dec = abs(lat-lat_deg) * 60
  lat_deg = abs(lat_deg)
  lat_hem = 'S'
  if lat > 0:
    lat_hem = 'N'
  return '%2d %05.2f %1s' % (lat_deg, lat_dec, lat_hem)
def dec_lng_to_woce_lng(lng):
  lng_deg = int(lng)
  lng_dec = abs(lng-lng_deg) * 60
  lng_deg = abs(lng_deg)
  lng_hem = 'W'
  if lng > 0 :
    lng_hem = 'E'
  return '%3d %05.2f %1s' % (lng_deg, lng_dec, lng_hem)

def strftime_iso(datetime):
  return datetime.isoformat()+'Z'

def strftime_woce_date_time(datetime):
  return (datetime.strftime('%Y%m%d'), datetime.strftime('%H%M'))

def out_of_band(value):
  try:
    number = float(value)
  except (ValueError):
    return False
  oob = -999
  tolerance = 0.1
  if abs(oob-number) < tolerance:
    return True
  return False

def grav_ocean_surface_wrt_latitude(latitude):
  return 9.780318*(1.0 + 5.2788e-3 * sin(latitude) ** 2 + 2.35e-5 * sin(latitude) ** 4)

# Following two functions ports of
# $Id: depth.c,v 11589a696ce7 2008/10/15 22:56:57 fdelahoyde $
# depth.c	1.1	Solaris 2.3 Unix	940906	SIO/ODF	fmd

DGRAV_DPRES = 2.184e-6 # Correction for gravity as pressure increases (closer
                       # to center of Earth

def depth(grav, p, rho):
  '''
  Calculate depth by integration of insitu density.

  Sverdrup, H. U.,Johnson, M. W., and Fleming, R. H., 1942.
  The Oceans, Their Physics, Chemistry and General Biology.
  Prentice-Hall, Inc., Englewood Cliff, N.J.

  grav - local gravity (m/sec^2) @ 0.0 db
  p - pressure series (decibars)
  rho - insitu density series (kg/m^3)

  Returns depth - depth series (meters)
  '''
  depth = []

  num_intervals = len(p)
  if not (num_intervals is len(rho)):
    raise ValueError("The number of series intervals must be the same.")

  # When calling depth() repeatedly with a two-element
  # series, the first call should be with a one-element series to
  # initialize the starting value (see depth_(), below).

  # Initialize the series
  if num_intervals is not 2:
    # If the integration starts from > 15 db, calculate depth relative to
    # starting place. Otherwise, calculate from surface.
    if p[0] > 15.0:
      depth.insert(0, 0.0)
    else:
      depth.insert(0, p[0]/(rho[0]*10000.0*(grav+DGRAV_DPRES*p[0])))
  # Calculate the rest of the series.
  for i in range(0, num_intervals-1):
    j = i+1
    # depth in meters
    depth.insert(j, depth[i] + (p[j]-p[i]) / ((rho[j]+rho[i])*5000.0*(grav+DGRAV_DPRES*p[j]))*1e8)
  return depth

def polynomial(x, coeffs):
  '''Gives the result of calculating coeffs[n]*x**n + coeffs[n-1]*x**n-1 + ... + coeffs[0]'''
  if len(coeffs) <= 0:
    return 0
  sum = coeffs[0]
  degreed = x
  for coef in coeffs[1:]:
    sum += coef * degreed
    degreed *= x
  return sum

def secant_bulk_modulus(salinity, temperature, pressure):
  '''Obtained from EOS80 according to Fofonoff Millard 1983 pg 15
     Parameters:
       salinity - [PSS-78]
       temperature - [degrees Celsius IPTS-68]
       pressure - pressure
  '''
  t = temperature
  if pressure == 0:
    E = (19652.21, 148.4206, -2.327105, 1.360477e-2, -5.155288e-5)
    Kw = polynomial(t, E)
    F = (54.6746, -0.603459, 1.09987e-2, -6.1670e-5)
    G = (7.944e-2, 1.6483e-2, -5.3009e-4)
    return Kw + polynomial(t, F)*salinity + polynomial(t, G)*salinity**(3.0/2.0)
  H = (3.239908, 1.43713e-3, 1.16092e-4, -5.77905e-7)
  Aw = polynomial(t, H)
  I = (2.2838e-3, -1.0981e-5, -1.6078e-6)
  j0 = 1.91075e-4
  A = Aw + polynomial(t, I)*salinity + j0 * salinity**(3.0/2.0)

  K = (8.50935e-5, -6.12293e-6, 5.2787e-8)
  Bw = polynomial(t, K)
  M = (-9.9348e-7, 2.0816e-8, 9.1697e-10)
  B = Bw + polynomial(t, M)*salinity
  return polynomial(pressure, (secant_bulk_modulus(salinity, temperature, 0), A, B))

def density(salinity, temperature, pressure):
  t = float(temperature)
  if pressure == 0:
    A = (999.842594, 6.793952e-2, -9.095290e-3, 1.001685e-4, -1.120083e-6, 6.536332e-9)
    pw = polynomial(t, A)
    B = (8.24493e-1, -4.0899e-3, 7.6438e-5, -8.2467e-7, 5.3875e-9)
    C = (-5.72466e-3, 1.0227e-4, -1.6546e-6)
    d0 = 4.8314e-4
    return pw + polynomial(t, B)*salinity + polynomial(t, C)*salinity**(3.0/2.0) + d0*salinity**2
  pressure /= 10 # Strange correction of one order of magnitude needed?
  return density(salinity, t, 0) / (1-(pressure/secant_bulk_modulus(salinity, t, pressure)))

def depth_unesco(pres, lat):
  '''
  Depth (meters) from pressure (decibars) using Saunders and Fofonoff's method.

  Saunders, P. M., 1981. Practical Conversion of Pressure to Depth.
  Journal of Physical Oceanography 11, 573-574.
  Mantyla, A. W., 1982-1983. Private correspondence.

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
    if parameter_name.startswith('_'):
      self.full_name = parameter_name
      self.format = '11s'
      self.units = 0
      self.bound_lower = None
      self.bound_upper = None
      self.units_mnemonic = ''
      self.woce_mnemonic = parameter_name
      self.display_order = -9999
      self.aliases = []
    else:
      try:
        self.init_from_postgresql(parameter_name)
      except pgdb.Error:
        try:
          self.init_from_mysql(parameter_name)
        except MySQLdb.Error:
          raise EnvironmentError("No databases could be used for parameter verification")
  def init_from_postgresql(self, parameter_name):
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
      self.format = row[1].strip()
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
  def init_from_mysql(self, parameter_name):
    connection = connect_mysql()
    cursor = connection.cursor()
    select = ','.join(['FullName', 'RubyPrecision', 'Description', 'Units',
                       'Range', 'Unit_Mnemonic', 'Alias'])
    cursor.execute("SELECT "+select+"""
                    FROM parameter_descriptions
                    WHERE Parameter LIKE %s
                    LIMIT 1""", (parameter_name,))
    row = cursor.fetchone()
    if row:
      self.full_name = row[0]
      self.format = row[1].strip()
      self.description = row[2]
      self.units = row[3]
      self.bound_lower = row[4].split(',')[0]
      self.bound_upper = row[4].split(',')[1]
      self.units_mnemonic = row[5]
      self.woce_mnemonic = parameter_name
      self.display_order = -9999
      self.aliases = row[6].split(',')
    else:
      connection.close()
      raise NameError("'"+parameter_name+"' is not in CCHDO's parameter list.")
    connection.close()
  def __eq__(self, other):
    return self.woce_mnemonic == other.woce_mnemonic
  def __str__(self):
    return 'Parameter '+self.woce_mnemonic

class Column:
  def __init__(self, parameter, contrived=False):
    if isinstance(parameter, Parameter):
      self.parameter = parameter
    else:
      if contrived:
        parameter = '_'+parameter
      self.parameter = Parameter(parameter)
    self.values = []
    self.flags_woce = []
    self.flags_igoss = []
  def get(self, index):
    if index >= len(self.values):
      return None
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

class SummaryFile:
  def __init__(self):
    self.columns = {}
    self.header = ''
    columns = ['EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO', 'DATE', 'TIME',
               'LATITUDE', 'LONGITUDE', 'DEPTH', '_CAST_TYPE', '_CODE',
               '_NAV', '_WIRE_OUT', '_ABOVE_BOTTOM', '_MAX_PRESSURE',
               '_NUM_BOTTLES', '_PARAMETERS', '_COMMENTS']
    for column in columns:
      self.columns[column] = Column(column)
  def __len__(self):
    if not self.columns.values():
      return 0
    return len(self.columns.values()[0])
  def read_Summary_WOCE(self, handle):
    '''How to read a Summary file for WOCE.'''
    header = True
    header_delimiter = compile('^-+$')
    column_starts = []
    column_widths = []
    for line in handle:
      if header:
        if header_delimiter.match(line):
          header = False
          # Stops are tuples (beginning of column, end of column)
          # This is to delimit the columns of the sumfile
          stops = finditer('\w+\s*', self.header.split('\n')[-2])
          for stop in stops:
            start = stop.group(0)
            if len(column_starts) is 0:
              column_starts.append(0)
            column_widths.append(stop.group(1)-start)
        else:
          self.header += line
      else:
        tokens = []
        for s, w in zip(column_starts, column_widths):
          tokens.append(line[:-1][s:s+w].strip())
        def identity_or_none(x):
          return x if x else None
        def int_or_none(x):
          return int(x) if x and x.isdigit() else None
        if len(tokens) is 0: continue
        self.columns['EXPOCODE'].append(tokens[0].replace('/', '_'))
        self.columns['SECT_ID'].append(tokens[1])
        self.columns['STNNBR'].append(int_or_none(tokens[2]))
        self.columns['CASTNO'].append(int_or_none(tokens[3]))
        self.columns['_CAST_TYPE'].append(tokens[4])
        date = datetime.strptime(tokens[5], '%m%d%y')
        self.columns['DATE'].append('%4d%02d%02d' % (date.year, date.month, date.day))
        self.columns['TIME'].append(int_or_none(tokens[6]))
        self.columns['_CODE'].append(tokens[7])
        lat = woce_lat_to_dec_lat(tokens[8].split())
        self.columns['LATITUDE'].append(lat)
        lng = woce_lng_to_dec_lng(tokens[9].split())
        self.columns['LONGITUDE'].append(lng)
        self.columns['_NAV'].append(tokens[10])
        self.columns['DEPTH'].append(int_or_none(tokens[11]))
        self.columns['_ABOVE_BOTTOM'].append(int_or_none(tokens[12]))
        self.columns['_WIRE_OUT'].append(int_or_none(tokens[13]))
        self.columns['_MAX_PRESSURE'].append(int_or_none(tokens[14]))
        self.columns['_NUM_BOTTLES'].append(int_or_none(tokens[15]))
        self.columns['_PARAMETERS'].append(identity_or_none(tokens[16]))
        self.columns['_COMMENTS'].append(identity_or_none(tokens[17]))
  def write_Summary_WOCE(self, handle):
    '''How to write a Summary file for WOCE.'''
    today = date.today()
    uniq_sects = uniquify(self.columns['SECT_ID'].values)
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
  def read_Summary_HOT(self, handle):
    '''How to read a Summary file for HOT.'''
    header = True
    header_delimiter = compile('^-+$')
    for line in handle:
      if header:
        if header_delimiter.match(line):
          header = False
        else:
          self.header += line
      else:
        # TODO Reimplement by finding ASCII column edges in header and reading that way.
        # Spacing is unreliable.
        tokens = line.split()
        if len(tokens) is 0: continue
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
  def write_Summary_HOT(self, handle):
    raise NotImplementedError # OMIT
  def write_nav(self, handle): # TODO consolidate with DataFile's write_nav? Same code.
    nav = uniquify(map(lambda coord: '%3.3f\t%3.3f\t%d\t%s\t%s\n' % coord, zip(self.columns['LONGITUDE'].values,
                                                                           self.columns['LATITUDE'].values,
                                                                           self.columns['STNNBR'].values,
                                                                           self.columns['DATE'].values,
                                                                           self.columns['_CODE'].values)))
    handle.write(''.join(nav))

class DataFile:
  def __init__(self):
    self.columns = {}
    self.stamp = None
    self.header = ''
    self.footer = None
    self.globals = {}
    self.allow_contrived = False
  def expocodes(self):
    return uniquify(self.columns['EXPOCODE'].values)
  def __len__(self):
    if not self.columns.values():
      return 0
    return len(self.columns.values()[0])
  def sorted_columns(self):
    return sorted(self.columns.values())
  def get_property_for_columns(self, property_getter):
    return map(property_getter, self.sorted_columns())
  def column_headers(self):
    return self.get_property_for_columns(lambda column: column.parameter.woce_mnemonic)
  def formats(self):
    return self.get_property_for_columns(lambda column: column.parameter.format)
  def to_hash(self):
    hash = {}
    for column in self.columns:
      hash[self.columns[column].parameter.woce_mnemonic] = self.columns[column].values
      hash[self.columns[column].parameter.woce_mnemonic+'_FLAG_W'] = self.columns[column].flags_woce
      hash[self.columns[column].parameter.woce_mnemonic+'_FLAG_I'] = self.columns[column].flags_igoss
    return hash

  # Refactored common code
  def create_columns(self, parameters, units):
    for parameter, unit in zip(parameters, units):
      if parameter.endswith('FLAG_W') or parameter.endswith('FLAG_I'): continue
      try:
        self.columns[parameter] = Column(parameter)
      except NameError, e:
        if self.allow_contrived:
          self.columns[parameter] = Column(parameter, True);
        else:
          raise e
      expected_units = self.columns[parameter].parameter.units_mnemonic
      if expected_units != unit:
        warn("Mismatched expected units '%s' with given units '%s'" % (expected_units, unit))
  def read_WOCE_data(self, handle, parameters_line, units_line, asterisk_line):
    column_width = 8
    safe_column_width = column_width-1
    num_quality_flags = len(findall('\*{7,8}', asterisk_line)) # i.e. the number of asterisk-marked columns
    num_quality_words = len(parameters_line.split('QUALT'))-1
    quality_length = num_quality_words * (num_quality_flags+1) # The extra 1 is for spacing between the columns
    num_param_columns = int((len(parameters_line) - quality_length) / column_width)

    # Unpack the column headers
    unpack_str = '8s' * num_param_columns
    parameters = strip_all(unpack(unpack_str, parameters_line[:num_param_columns*8]))
    units = strip_all(unpack(unpack_str, units_line[:num_param_columns*8]))
    asterisks = strip_all(unpack(unpack_str, asterisk_line[:num_param_columns*8]))

    # Warn if the header lines break 8 character column rules
    for parameter in parameters:
      if len(parameter) > safe_column_width:
        warn("Parameter '%s' has too many characters (>%d)." % (parameter, safe_column_width))
    for unit in units:
      if len(unit) > safe_column_width:
        warn("Unit '%s' has too many characters (>%d)." % (parameter, safe_column_width))
    for asterisk in asterisks:
      if len(asterisk) > safe_column_width:
        warn("Asterisks '%s' has too many characters (>%d)." % (parameter, safe_column_width))

    # Die if parameters are not unique
    if not parameters == uniquify(parameters):
      raise ValueError('There were duplicate parameters in the file; cannot continue without data corruption.')

    self.create_columns(parameters, units)

    # Get each data line
    unpack_str += ('x'+str(num_quality_flags)+'s') * num_quality_words # Add on quality to unpack string
    for line in handle:
      unpacked = unpack(unpack_str, line.rstrip())

      # QUALT1 takes precedence
      quality_flags = unpacked[-num_quality_words:]

      # Build up the columns for the line
      flag_i = 0
      for i, parameter in enumerate(parameters):
        datum = float(unpacked[i])
        if datum is -9.0: datum = NaN
        woce_flag = None
        if not asterisks[i].strip() == '': # Only assign flag if column is flagged.
          woce_flag = int(quality_flags[0][flag_i])
          flag_i += 1
        self.columns[parameter].set(i, datum, woce_flag)

    # Expand globals into columns
    #@header.each_pair do |header, value|
    #  column = @column_hash[header] = Column.new(header)
    #  column.values = Array.new(num_entries) {|i| value}

  # IO methods
  def read_db(self):
    raise NotImplementedError # TODO
  def write_db(self):
    raise NotImplementedError # TODO
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
    nav = uniquify(map(lambda coord: '%3.3f\t%3.3f\t%d\t%s\t%s\n' % coord, zip(self.columns['LONGITUDE'].values,
                                                                           self.columns['LATITUDE'].values,
                                                                           self.columns['STNNBR'].values,
                                                                           map(lambda d: d.strftime('%Y-%m-%d'), self.columns['_DATETIME'].values),
                                                                           ['BO'] * len(self))))
    handle.write(''.join(nav))
  def read_CTD_WOCE(self, handle): # TODO Out of band values should be converted to None
    '''How to read a CTD WOCE file.'''
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
  def write_CTD_WOCE(self, handle):
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

    self.create_columns(columns, units)

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
    # Map the nc_ctd variable to drop to skip the variable.
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
        # CCHDO NetCDFs have STNNBR and CASTNO as an array of characters.
        # Collapse them into a string.
        if name in ['STNNBR', 'CASTNO']:
          self.columns[name].values = [''.join(self.columns[name].values)]
        # Translate string date YYYYMMDD to date object
        if name in ['DATE']:
          string = str(self.columns[name].values[0])
          self.columns[name].values[0] = '%s-%s-%s' % (string[0:4], string[4:6], string[6:8])
        # If the column has only one data point it should be in the globals
        if len(self.columns[name].values) <= 1:
          self.globals[name] = self.columns[name].get(0)
          del self.columns[name]
    # Second pass to put in flags
    for name, variable in qc_vars.items():
      if name in self.columns:
        self.columns[name].flags_woce = variable[:].tolist()
      else:
        # The column is probably a global
        pass
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
    raise NotImplementedError # TODO
  def read_CTD_NetCDF_OceanSITES(self, handle):
    '''How to read a CTD NetCDF OceanSITES file.'''
    raise NotImplementedError # TODO
  def write_CTD_NetCDF_OceanSITES(self, handle):
    '''How to write a CTD NetCDF OceanSITES file.'''
    filename = handle.name
    handle.close()
    strdate = str(self.globals['DATE']) 
    strtime = str(self.globals['TIME']).rjust(4, '0')
    isowocedate = datetime(int(strdate[0:4]), int(strdate[5:7]), int(strdate[8:10]),
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
    nc_file.createDimension('DEPTH', len(self))
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

    var_depth = nc_file.createVariable('DEPTH', 'f', ('DEPTH',))
    var_depth.long_name = 'Depth of each measurement'
    var_depth.standard_name = 'depth'
    var_depth.units = 'meters'
    var_depth._FillValue = -99999.0
    var_depth.valid_min = 0.0
    var_depth.valid_max = 12000.0
    var_depth.QC_indicator = 8 # Subject: OceanSITES: more on QC flags, uncertainty, depth Interpolated from latitude and pressure.
    var_depth.QC_procedure = 2 # See above
    var_depth.uncertainty = 1.0 # A decibar
    var_depth.axis = 'down' # oceanic

    since_1950 = isowocedate - datetime(1950, 1, 1)
    var_time[:] = [since_1950.days + since_1950.seconds/86400.0]
    var_latitude[:] = [self.globals['LATITUDE']]
    var_longitude[:] = [self.globals['LONGITUDE']]

    # CTD variables
    param_to_oceansites = {
      'ctd_pressure': 'PRES',
      'ctd_temperature': 'TEMP',
      'ctd_oxygen': 'DOXY',
      'ctd_salinity': 'PSAL',
    }
    oceansites_variables = {
      'TEMP': {'long': 'sea water temperature', 'std': 'sea_water_temperature', 'units': 'degree_Celsius'},
      'DOXY': {'long': 'dissolved oxygen', 'std': 'dissolved_oxygen', 'units': 'micromole/kg'},
      'PSAL': {'long': 'sea water salinity', 'std': 'sea_water_salinity', 'units': 'psu'},
      'PRES': {'long': 'sea water pressure', 'std': 'sea_water_pressure', 'units': 'decibars'}, # valid_min 0.0, valid_max 12000.0, QC_indicator =7, QC_procedure = 5, uncertainty 2.0
    }
    oceansites_uncertainty = {
      'TEMP': 0.002,
      'PSAL': 0.005,
      'DOXY': Infinity,
      'PRES': Infinity,
    }

    for column in self.columns.values():
      name = column.parameter.description.lower().replace(' ', '_').replace('(', '_').replace(')', '_')
      if name in param_to_oceansites.keys():
        name = param_to_oceansites[name]
      # Write variable
        var = nc_file.createVariable(name, 'f8', ('DEPTH',))
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
      if name is 'PRES':
        # Fun using Sverdrup's depth integration with density.
        localgrav = grav_ocean_surface_wrt_latitude(self.globals['LATITUDE'])
        density_series = [density(*args) for args in zip(self.columns['CTDSAL'].values, self.columns['CTDTMP'].values, column.values)]
        depth_series = depth(localgrav, column.values, density_series)

        #depth_series = map(lambda pres: depth_unesco(pres, self.globals['LATITUDE']), self.columns['CTDPRS'].values)

        var_depth[:] = depth_series
      # Write QC variable
      if column.is_flagged_woce():
        var.ancillary_variables = name+'_QC'
        flag = nc_file.createVariable(name+'_QC', 'b', ('DEPTH',))
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
    isodate = datetime(int(strdate[0:4]), int(strdate[5:7]), int(strdate[8:10]), 0, 0)
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
    isodate = datetime(int(strdate[0:4]), int(strdate[5:7]), int(strdate[8:10]), 0, 0)
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
    raise NotImplementedError # OMIT
  def read_Bottle_WOCE(self, handle):
    from libcchdo.bottle.woce import woce
    return woce(self).read(handle)
  def write_Bottle_WOCE(self, handle):
    from libcchdo.bottle.woce import woce
    return woce(self).write(handle)
  def read_Bottle_Exchange(self, handle):
    from bottle.exchange import exchange
    return exchange(self).read(handle)
  def write_Bottle_Exchange(self, handle):
    from bottle.exchange import exchange
    return exchange(self).write(handle)
  def read_Bottle_NetCDF(self, handle):
    from libcchdo.bottle.netcdf import netcdf
    return netcdf(self).read(handle)
  def write_Bottle_NetCDF(self, handle):
    from libcchdo.bottle.netcdf import netcdf
    return netcdf(self).write(handle)
  def write_Google_Wire(self, handle):
    from libcchdo.google_wire.google_wire import google_wire
    return google_wire(self).write(handle)

class DataFileCollection:
  def __init__(self):
    self.files = []
  def merge(datafile):
    raise NotImplementedError # TODO
  def split(self):
    raise NotImplementedError # TODO
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
    raise NotImplementedError # TODO
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
    raise NotImplementedError # OMIT
  def read_CTDZip_NetCDF(self, handle):
    '''How to read CTD NetCDF files from a Zip.'''
    raise NotImplementedError
  def write_CTDZip_NetCDF(self, handle):
    '''How to write CTD NetCDF files to a Zip.'''
    raise NotImplementedError
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

