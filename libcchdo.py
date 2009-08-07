#!/usr/bin/env python

# libcchdo Python
#
# Blame
# myshen 2009-08-06 Initial

from __future__ import with_statement
from datetime import date, datetime, now
import MySQLdb
from os import listdir, remove, rmdir
from re import compile
from sys import exit
from StringIO import StringIO
from tempfile import mkdtemp
from zipfile import ZipFile, ZipInfo

from Scientific.IO import NetCDF
from Numeric import *
from NetCDF import *

try:
  connection = MySQLdb.connect(host='cchdo.ucsd.edu',
                               user='cchdo_rails',
                               passwd='((hd0hydr0d@t@',
                               db='cchdo')
except MySQLdb.Error, e:
  print "Error %d: %s" % (e.args[0], e.args[1])
  exit(1)

class Parameter:
  def __init__(self, parameter_name):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM parameter_descriptions"+
                   "WHERE parameter = '"+parameter_name+"'"+
                   "LIMIT 1")
    if row = cursor.fetchone():
      print 'the row:', row
      self.full_name = ""
      self.description = ""
      self.units = ""
      self.woce_mnemonic = parameter_name
      self.units_mnemonic = ""
      self.range = ""
      self.precision = ""
      self.display_order = -9999
      self.aliases = []
    else:
      raise NameError(parameter_name+
                      " is not in CCHDO's parameter list.")

class Column:
  def __init__(self, parameter_name):
    self.parameter = parameter_name # Not really. Will change.
    self.values = []
    self.flags_woce = []
    self.flags_igoss = []
  def get(self, index):
    return self.values[index]
  def set(self, index, value, flag_woce=None, flag_igoss=None):
    self.values.insert(index, value)
    if flag_woce:
      self.flags_woce.insert(index, flag_woce)
    if flag_igoss:
      self.flags_igoss.insert(index, flag_igoss)
  def __getitem__(self, key):
    return self.get(key)
  def __setitem__(self, key, value):
    self.values.insert(key, value)
  def __len__(self):
    return len(self.values)
  def is_flagged(self):
    return self.is_flagged_woce() or self.is_flagged_igoss()
  def is_flagged_woce(self):
    return not (self.flags_woce is None or len(self.flags_woce) == 0)
  def is_flagged_igoss(self):
    return not (self.flags_igoss is None or len(self.flags_igoss) == 0)
  def __cmp__(self, other):
    return self.parameter.display_order - other.parameter.display_order

class DataFile:
  def __init__(self):
    self.columns = {}
    self.stamp = None
    self.header = None
    self.footer = None
    self.globals = {}
  def read_db(self):
    pass
  def write_db(self):
    pass
  def column_headers(self):
    return self.columns.keys()
  def expocodes(self):
    def uniqify(seq): # Dave Kirby
      seen = set()
      return [x for x in seq if x not in seen and not seen.add(x)]
    return uniqify(columns['EXPOCODE'])
  def __len__(self):
    if not self.columns.values():
      return 0
    return len(self.columns.values()[0])
  def precisions(self):
    return map(lambda column: column.parameter.precision, sort(self.columns.values))
  def to_hash(self):
    pass # TODO

  # IO methods
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
  def write_CTD_Exchange(self, handle):
    '''How to write a CTD Exchange file.'''
    today = date.today()
    handle.write('CTD,'+str(today.year)+str(today.month)+str(today.day)+"SIOCCHDOLIB\n")
    handle.write('NUMBER_HEADERS = '+str(len(self.globals.keys())+1)+"\n")
    for key in self.globals.keys():
      handle.write(key+' = '+self.globals[key]+"\n")
    handle.write(','.join(self.column_headers())+"\n")
    for i in range(len(self)):
      handle.write(','.join(map(lambda header: self.columns[header][i],
                                 self.column_headers()))+"\n")
    handle.write("END_DATA\n")
  def read_CTD_NetCDF(self, handle):
    '''How to read a CTD NetCDF file.'''
    nc_file = NetCDFFile(handle, 'r')
    print 'variable names for file: ', nc_file.variables.keys()
    print 'variables: ', nc_file.variables.values()
    for name, variable in nc_file.variables.items():
      self.columns[name] = Column(name)
      self.columns[name].parameter = variable
    global_attrs = dir(nc_file)
    for attr in global_attrs:
      print attr
    nc_file.close()
  def write_CTD_NetCDF(self, handle):
    '''How to write a CTD NetCDF file.'''
    pass
  def read_Bottle_netCDF(self, handle):
    '''How to read a Bottle netCDF file.'''
    pass
  def write_Bottle_netCDF(self, handle):
    '''How to write a Bottle netCDF file.'''
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
    setattr(nc_file, 'Creation_Time', str(now()))
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
  def read_CTDZip_ODEN(self, handle):
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
  def write_CTDZip_Exchange(self, handle):
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
  def read_BottleZip_NetCDF(self, handle):
    '''How to read Bottle NetCDF files from a Zip.'''
    pass
  def write_BottleZip_NetCDF(self, handle):
    '''How to write Bottle NetCDF files to a Zip.'''
    # NetCDF libraries seem to like to write to a file themselves. We have to
    # work around that by determining a temp directory.
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

# Closing the database connection after all the definitions are given
connection.close()

from sys import argv
file = DataFileCollection()
file.read_CTD_netCDF(argv[1]) # NetCDF is a special case. It wants to write its own files.
#with open(argv[1], 'r') as in_file:
#  file.read_CTDZip_ODEN(in_file)
#with open(argv[2], 'w') as out_file:
  #file.write_CTDZip_Exchange(out_file)
  #file.write_Bottle_netCDF(out_file)
