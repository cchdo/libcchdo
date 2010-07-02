"""libcchdo Python

Internal Data Specification
---------------------------
Any unreported values must be represented as None. This includes -9, -999.000,
unspecified dates, times, etc.

Known unknown parameters have mnemonics that start with '_'. e.g. MAX PRESSURE
exists in certain files but there is no parameter defined for it. By prefixing
MAX_PRESSURE with a '_', the library will not retrive the parameter definition
from the database (there is none anyway).
"""

import datetime
import math
from warnings import warn
import os.path
import re
import struct

import db.connect
import formats

LIBVER = 'SIOCCHDLIB'

# Import netCDF here because there is no easy way to import it for specific
# formats. TODO find a way to import netcdf only for specific formats.
try:
    from netCDF3 import Dataset
except ImportError, e:
    raise ImportError('%s\n%s' % (e,
        ("You should get netcdf4-python from http://code.google.com/p/"
         "netcdf4-python and install the NetCDF 3 module as directed by the "
         "README.")))

try:
    from math import isnan
except ImportError: # Cover when < python-2.6
    def isnan(n):
        return n != n

# Globals
RADIUS_EARTH = 6371.01 #km

# Functions
def uniquify(seq):
    '''Order preserving uniquify.
       http://www.peterbe.com/plog/uniqifiers-benchmark/
         uniqifiers_benchmark.py (f8 by Dave Kirby)
    '''
    seen = set()
    a = seen.add
    return [x for x in seq if x not in seen and not a(x)]


def strip_all(list):
    return map(lambda x: x.strip(), list)


def read_arbitrary(filename):
    if not os.path.exists(filename):
        raise ValueError("The file '%s' does not exist" % filename)
    if filename.endswith('zip'):
        datafile = DataFileCollection()
    elif filename.endswith('su.txt'):
        datafile = SummaryFile()
    else:
        datafile = DataFile()

    if filename.endswith('su.txt'):
        datafile.read_Summary_WOCE(handle)
    elif filename.endswith('hy.txt'):
        formats.bottle.woce.woce(datafile).read(handle)
    elif filename.endswith('hy1.csv'):
        formats.bottle.exchange.exchange(datafile).read(handle)
    elif filename.endswith('nc_hyd.zip'):
        formats.bottle.zip.netcdf.netcdf(datafile).read(handle)
    elif filename.endswith('ct.zip'):
        formats.ctd.zip.woce.woce(datafile).read(handle)
    elif filename.endswith('ct1.zip'):
        formats.ctd.zip.exchange.exchange(datafile).read(handle)
    elif filename.endswith('nc_ctd.zip'):
        formats.ctd.zip.netcdf.netcdf(datafile).read(handle)
    else:
      raise ValueError('Unrecognized file type for %s' % filename)

    return datafile


def great_circle_distance(lat_stand, lng_stand, lat_fore, lng_fore):
    delta_lng = lng_fore - lng_stand
    cos_lat_fore = math.cos(lat_fore)
    cos_lat_stand = math.cos(lat_stand)
    cos_lat_fore_cos_delta_lng = cos_lat_fore * math.cos(delta_lng)
    sin_lat_stand = math.sin(lat_stand)
    sin_lat_fore = math.sin(lat_fore)

    # Vicenty formula from Wikipedia
    # fraction_top = sqrt( (cos_lat_fore * sin(delta_lng)) ** 2 +
    #                      (cos_lat_stand * sin_lat_fore -
    #                       sin_lat_stand * cos_lat_fore_cos_delta_lng) ** 2)
    # fraction_bottom = sin_lat_stand * sin_lat_fore +
    #                   cos_lat_stand * cos_lat_fore_cos_delta_lng
    # central_angle = atan2(1.0, fraction_top/fraction_bottom)

    # simple formula from wikipedia
    central_angle = math.acos(cos_lat_stand * cos_lat_fore * \
                              math.cos(delta_lng) + \
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


def strftime_iso(dtime):
    return dtime.isoformat()+'Z'


def strftime_woce_date_time(dtime):
    return (dtime.strftime('%Y%m%d'), dtime.strftime('%H%M'))


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
    return 9.780318 * (1.0 + 5.2788e-3 * math.sin(latitude) ** 2 +
                             2.35e-5 * math.sin(latitude) ** 4)

# Following two functions ports of
# $Id: depth.c,v 11589a696ce7 2008/10/15 22:56:57 fdelahoyde $
# depth.c	1.1	Solaris 2.3 Unix	940906	SIO/ODF	fmd

DGRAV_DPRES = 2.184e-6 # Correction for gravity as pressure increases (closer
                       # to center of Earth


def depth(grav, p, rho):
    """Calculate depth by integration of insitu density.

    Sverdrup, H. U.,Johnson, M. W., and Fleming, R. H., 1942.
    The Oceans, Their Physics, Chemistry and General Biology.
    Prentice-Hall, Inc., Englewood Cliff, N.J.

    Args:
        grav: local gravity (m/sec^2) @ 0.0 db
        p: pressure series (decibars)
        rho: insitu density series (kg/m^3)

    Returns:
        depth - depth series (meters)
    """
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
            depth.append(0.0)
        else:
            depth.append(p[0] / (rho[0] * 10000.0 * \
                                 (grav + DGRAV_DPRES * p[0])))

    # Calculate the rest of the series.
    for i in range(0, num_intervals - 1):
        j = i + 1
        # depth in meters
        depth.insert(j, depth[i] + (p[j] - p[i]) / \
                                   ((rho[j] + rho[i]) * 5000.0 * \
                                    (grav + DGRAV_DPRES * p[j])) * 1e8)

    return depth


def polynomial(x, coeffs):
    """Calculate a polynomial.
    
    Gives the result of calculating
    coeffs[n]*x**n + coeffs[n-1]*x**n-1 + ... + coeffs[0]
    """
    if len(coeffs) <= 0:
        return 0
    sum = coeffs[0]
    degreed = x
    for coef in coeffs[1:]:
        sum += coef * degreed
        degreed *= x
    return sum


def secant_bulk_modulus(salinity, temperature, pressure):
    """Calculate the secant bulk modulus of sea water.
    
    Obtained from EOS80 according to Fofonoff Millard 1983 pg 15

    Args:
        salinity: [PSS-78]
        temperature: [degrees Celsius IPTS-68]
        pressure: pressure

    Returns:
        The secant bulk modulus of sea water as a float.
    """
    t = temperature

    if pressure == 0:
        E = (19652.21, 148.4206, -2.327105, 1.360477e-2, -5.155288e-5)
        Kw = polynomial(t, E)
        F = (54.6746, -0.603459, 1.09987e-2, -6.1670e-5)
        G = (7.944e-2, 1.6483e-2, -5.3009e-4)
        return Kw + polynomial(t, F) * salinity + \
               polynomial(t, G) * salinity ** (3.0 / 2.0)
    H = (3.239908, 1.43713e-3, 1.16092e-4, -5.77905e-7)
    Aw = polynomial(t, H)
    I = (2.2838e-3, -1.0981e-5, -1.6078e-6)
    j0 = 1.91075e-4
    A = Aw + polynomial(t, I) * salinity + j0 * salinity ** (3.0 / 2.0)

    K = (8.50935e-5, -6.12293e-6, 5.2787e-8)
    Bw = polynomial(t, K)
    M = (-9.9348e-7, 2.0816e-8, 9.1697e-10)
    B = Bw + polynomial(t, M) * salinity
    return polynomial(pressure,
                      (secant_bulk_modulus(salinity, temperature, 0), A, B))


def density(salinity, temperature, pressure):
    t = float(temperature)

    if pressure == 0:
        A = (999.842594, 6.793952e-2, -9.095290e-3,
             1.001685e-4, -1.120083e-6, 6.536332e-9)
        pw = polynomial(t, A)
        B = (8.24493e-1, -4.0899e-3, 7.6438e-5, -8.2467e-7, 5.3875e-9)
        C = (-5.72466e-3, 1.0227e-4, -1.6546e-6)
        d0 = 4.8314e-4
        return pw + polynomial(t, B) * salinity + \
               polynomial(t, C) * salinity ** (3.0 / 2.0) + d0 * salinity ** 2
    pressure /= 10 # Strange correction of one order of magnitude needed?
    return density(salinity, t, 0) / \
           (1 - (pressure / secant_bulk_modulus(salinity, t, pressure)))


def depth_unesco(pres, lat):
    """Depth (meters) from pressure (decibars) using
    Saunders and Fofonoff's method.

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
    Checkvalue: depth = 9712.653 M for P=10000 decibars,
                latitude=30 deg above
      for standard ocean: T=0 deg celsius; S=35 (PSS-78)
    """

    x = math.sin(lat / 57.29578) ** 2
    gr = 9.780318 * (1.0 + (5.2788e-3 + 2.36e-5 * x) * x) + 1.092e-6 * pres
    return ((((-1.82e-15 * pres + 2.279e-10) * pres - 2.2512e-5) * \
           pres + 9.72659) * pres) / gr


KNOWN_PARAMETERS = {
    'EXPOCODE': {'name': 'ExpoCode',
                 'format': '11s',
                 'description': 'ExpoCode',
                 'units': '',
                 'bound_lower': '',
                 'bound_upper': '',
                 'mnemonic': 'EXPOCODE',
                 'display_order': 1,
                 'aliases': [],
                },
    'SECT_ID': {'name': 'Section ID',
                'format': '11s',
                'description': 'Section ID',
                'units': '',
                'bound_lower': '',
                'bound_upper': '',
                'mnemonic': 'SECT_ID',
                'display_order': 2,
                'aliases': [],
               },
# The CTD details are included because the database does not have descriptions.
    'CTDPRS': {'name': 'Pressure',
               'format': '8.1f',
               'description': 'CTD pressure',
               'units': 'decibar',
               'bound_lower': '0',
               'bound_upper': '11000',
               'mnemonic': 'DBAR',
               'display_order': 6,
               'aliases': [],
              },
    'CTDTMP': {'name': 'Temperature',
               'format': '8.4f',
               'description': 'CTD temperature',
               'units': 'ITS90',
               'bound_lower': '-2',
               'bound_upper': '35',
               'mnemonic': 'ITS-90',
               'display_order': 7,
               'aliases': [],
              },
    'CTDOXY': {'name': 'Oxygen',
               'format': '8.1f',
               'description': 'CTD oxygen',
               'units': u'\xb5mol/kg',
               'bound_lower': '0',
               'bound_upper': '500',
               'mnemonic': 'UMOL/KG',
               'display_order': 8,
               'aliases': [],
              },
    'CTDSAL': {'name': 'Salinity',
               'format': '8.4f',
               'description': 'CTD salinity',
               'units': 'PSS-78',
               'bound_lower': '0',
               'bound_upper': '42',
               'mnemonic': 'PSS-78',
               'display_order': 9,
               'aliases': [],
              },
    'CTDNOBS': {'name': 'nobs', # XXX
               'format': 's',
               'description': 'Number of observations',
               'units': '',
               'bound_lower': '',
               'bound_upper': '',
               'mnemonic': '',
               'display_order': float("Inf"),
               'aliases': [],
              },
}


class Parameter:

    def __init__(self, parameter_name, contrived=False):
        if contrived or parameter_name.startswith('_'):
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
            #try:
            #    self.init_from_postgresql(parameter_name)
            #except Exception, e:
            #    warn(("%s\nFalling back to mysql database for "
            #          "parameter info.") % e)
            try:
                self.init_from_mysql(parameter_name)
            except Exception, e:
                raise EnvironmentError(
                    ("%s\nNo databases could be used for "
                     "parameter verification.") % e)

    def init_from_postgresql(self, parameter_name):
        connection = db.connect.cchdotest()
        cursor = connection.cursor()
        select = ','.join(
            ('parameters.name', 'format', 'description', 'units', 'bound_lower',
             'bound_upper', 'units.mnemonic_woce', 'parameters_orders.order',))
        cursor.execute(
            ('SELECT %s FROM parameters '
             'INNER JOIN parameters_aliases ON '
             'parameters.id = parameters_aliases.parameter_id '
             'LEFT JOIN parameters_orders ON '
             'parameters.id = parameters_orders.parameter_id '
             'LEFT JOIN units ON parameters.units = units.id '
             "WHERE parameters_aliases.name = '%s' "
             'LIMIT 1') % (select, parameter_name,))
        row = cursor.fetchone()
        if row:
            self.full_name = row[0]
            self.format = row[1].strip() if row[1] else '11s'
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
            raise NameError(
                 "'%s' is not in CCHDO's parameter list." % parameter_name)
        connection.close()

    def init_from_mysql(self, parameter_name):
        if parameter_name in KNOWN_PARAMETERS:
            info = KNOWN_PARAMETERS[parameter_name]
            self.full_name = info['name']
            self.format = info['format']
            self.description = info['description']
            self.units = info['units']
            self.bound_lower = info['bound_lower']
            self.bound_upper = info['bound_upper']
            self.units_mnemonic = info['mnemonic']
            self.woce_mnemonic = parameter_name
            self.display_order = info['display_order']
            self.aliases = info['aliases']
            return
        connection = db.connect.cchdo()
        cursor = connection.cursor()
        def wrap_column(s):
            return '`%s`' % s
        select = ','.join(map(wrap_column,
                              ('FullName', 'RubyPrecision', 'Description',
                               'Units', 'Range', 'Unit_Mnemonic', 'Alias',)))
        cursor.execute(
            ('SELECT %s FROM parameter_descriptions '
             "WHERE Parameter LIKE '%s' LIMIT 1") % (select, parameter_name,))
        row = cursor.fetchone()
        if row:
            self.full_name = row[0]
            self.format = row[1].strip() if row[1] else '11s'
            self.description = row[2] or ''
            self.units = row[3]
            self.bound_lower = row[4].split(',')[0] if row[4] else None
            self.bound_upper = row[4].split(',')[1] if row[4] else None
            self.units_mnemonic = row[5]
            self.woce_mnemonic = parameter_name
            self.display_order = -9999
            self.aliases = row[6].split(',') if row[6] else []
            connection.close()
        else:
            connection.close()
            raise NameError(
                 "'%s' is not in CCHDO's parameter list." % parameter_name)

    def __eq__(self, other):
        return self.woce_mnemonic == other.woce_mnemonic

    def __str__(self):
        return 'Parameter %s' % self.woce_mnemonic


class Column:

   def __init__(self, parameter, contrived=False):
       if isinstance(parameter, Parameter):
           self.parameter = parameter
       else:
           self.parameter = Parameter(parameter, contrived)
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
        columns = ("EXPOCODE SECT_ID STNNBR CASTNO DATE TIME LATITUDE "
                   "LONGITUDE DEPTH _CAST_TYPE _CODE _NAV _WIRE_OUT "
                   "_ABOVE_BOTTOM _MAX_PRESSURE _NUM_BOTTLES "
                   "_PARAMETERS _COMMENTS").split()
        for column in columns:
            self.columns[column] = Column(column)

    def __len__(self):
        if not self.columns.values():
            return 0
        return len(self.columns.values()[0])

    def read_Summary_WOCE(self, handle):
        '''How to read a Summary file for WOCE.'''
        header = True
        header_delimiter = re.compile('^-+$')
        column_starts = []
        column_widths = []
        for line in handle:
            if header:
                if header_delimiter.match(line):
                    header = False
                    # Stops are tuples (beginning of column, end of column)
                    # This is to delimit the columns of the sumfile
                    stops = re.finditer('(\w+\s*)', self.header.split('\n')[-2])
                    for stop in stops:
                        start = stop.start()
                        if len(column_starts) is 0:
                            column_starts.append(0)
                        else:
                            column_starts.append(start)
                        column_widths.append(stop.end()-start)
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
                cs = self.columns
                cs['EXPOCODE'].append(tokens[0].replace('/', '_'))
                cs['SECT_ID'].append(tokens[1])
                cs['STNNBR'].append(int_or_none(tokens[2]))
                cs['CASTNO'].append(int_or_none(tokens[3]))
                cs['_CAST_TYPE'].append(tokens[4])
                date = datetime.datetime.strptime(tokens[5], '%m%d%y')
                cs['DATE'].append('%4d%02d%02d' % \
                                  (date.year, date.month, date.day))
                cs['TIME'].append(int_or_none(tokens[6]))
                cs['_CODE'].append(tokens[7])
                lat = woce_lat_to_dec_lat(tokens[8].split())
                cs['LATITUDE'].append(lat)
                lng = woce_lng_to_dec_lng(tokens[9].split())
                cs['LONGITUDE'].append(lng)
                cs['_NAV'].append(tokens[10])
                cs['DEPTH'].append(int_or_none(tokens[11]))
                cs['_ABOVE_BOTTOM'].append(int_or_none(tokens[12]))
                cs['_WIRE_OUT'].append(int_or_none(tokens[13]))
                cs['_MAX_PRESSURE'].append(int_or_none(tokens[14]))
                cs['_NUM_BOTTLES'].append(int_or_none(tokens[15]))
                cs['_PARAMETERS'].append(identity_or_none(tokens[16]))
                cs['_COMMENTS'].append(identity_or_none(tokens[17]))

    def write_Summary_WOCE(self, handle):
        '''How to write a Summary file for WOCE.'''
        today = datetime.date.today()
        uniq_sects = uniquify(self.columns['SECT_ID'].values)
        handle.write('R/V _SHIP LEG _# WHP-ID '+','.join(uniq_sects)+
                     ' %04d%02d%02d' % (today.year, today.month, today.day)+
                     "SIOCCHDOLIB\n")
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
              (self.columns['EXPOCODE'][i], self.columns['SECT_ID'][i],
               self.columns['STNNBR'][i], self.columns['CASTNO'][i],
               self.columns['_CAST_TYPE'][i], date_str,
               self.columns['TIME'][i], self.columns['_CODE'][i],
               dec_lat_to_woce_lat(self.columns['LATITUDE'][i]),
               dec_lng_to_woce_lng(self.columns['LONGITUDE'][i]),
               self.columns['_NAV'][i], self.columns['DEPTH'][i],
               self.columns['_ABOVE_BOTTOM'][i],
               self.columns['_MAX_PRESSURE'][i],
               self.columns['_NUM_BOTTLES'][i], self.columns['_PARAMETERS'][i],
               self.columns['_COMMENTS'][i]))
            handle.write(row+'\n')
        handle.close()

    def read_Summary_HOT(self, handle):
        '''How to read a Summary file for HOT.'''
        header = True
        header_delimiter = re.compile('^-+$')
        for line in handle:
            if header:
                if header_delimiter.match(line):
                    header = False
                else:
                    self.header += line
            else:
              # TODO Reimplement by finding ASCII column edges in header and
              # reading that way. 
              # Spacing is unreliable.
              tokens = line.split()
              if len(tokens) is 0:
                  continue
              self.columns['EXPOCODE'].append(tokens[0].replace('/', '_'))
              self.columns['SECT_ID'].append(tokens[1])
              self.columns['STNNBR'].append(int(tokens[2]))
              self.columns['CASTNO'].append(int(tokens[3]))
              self.columns['_CAST_TYPE'].append(tokens[4])
              date = datetime.datetime.strptime(tokens[5], '%m%d%y')
              self.columns['DATE'].append(
                  "%4d%02d%02d" % (date.year, date.month, date.day))
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


class DataFile:

    def __init__(self, allow_contrived=False):
        self.columns = {}
        self.stamp = None
        self.header = ''
        self.footer = None
        self.globals = {}
        self.allow_contrived = allow_contrived

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
        return self.get_property_for_columns(
            lambda column: column.parameter.woce_mnemonic)

    def formats(self):
        return self.get_property_for_columns(
            lambda column: column.parameter.format)

    def to_hash(self):
        hash = {}
        for column in self.columns:
            woce = self.columns[column].parameter.woce_mnemonic 
            hash[woce] = self.columns[column].values
            hash[woce+'_FLAG_W'] = self.columns[column].flags_woce
            hash[woce+'_FLAG_I'] = self.columns[column].flags_igoss
        return hash

    # Refactored common code

    def create_columns(self, parameters, units):
        for parameter, unit in zip(parameters, units):
            if parameter.endswith('FLAG_W') or parameter.endswith('FLAG_I'):
                continue
            try:
                self.columns[parameter] = Column(parameter,
                                                 self.allow_contrived)
            except Exception, e:
                raise e
            expected_units = self.columns[parameter].parameter.units_mnemonic
            if expected_units != unit:
                warn(("Mismatched expected units '%s' "
                      "with given units '%s'") % (expected_units, unit))

    def read_WOCE_data(self, handle, parameters_line,
                       units_line, asterisk_line):
        column_width = 8
        safe_column_width = column_width-1
        # num_quality_flags = the number of asterisk-marked columns
        num_quality_flags = len(re.findall('\*{7,8}', asterisk_line))
        num_quality_words = len(parameters_line.split('QUALT'))-1
        # The extra 1 in quality_length is for spacing between the columns
        quality_length = num_quality_words * (num_quality_flags+1)
        num_param_columns = int((len(parameters_line) - quality_length) / \
                                 column_width)

        # Unpack the column headers
        unpack_str = '8s' * num_param_columns
        parameters = strip_all(struct.unpack(unpack_str,
                                      parameters_line[:num_param_columns*8]))
        units = strip_all(struct.unpack(unpack_str,
                                        units_line[:num_param_columns*8]))
        asterisks = strip_all(struct.unpack(unpack_str,
                                     asterisk_line[:num_param_columns*8]))

        # Warn if the header lines break 8 character column rules
        def warn_broke_character_column_rule(headername, headers):
            for header in headers:
                if len(header) > safe_column_width:
                    warn("%s '%s' has too many characters (>%d)." % \
                         (headername, header, safe_column_width))

        warn_broke_character_column_rule("Parameter", parameters)
        warn_broke_character_column_rule("Unit", units)
        warn_broke_character_column_rule("Asterisks", asterisks)

        # Die if parameters are not unique
        if not parameters == uniquify(parameters):
            raise ValueError(('There were duplicate parameters in the file; '
                              'cannot continue without data corruption.'))

        self.create_columns(parameters, units)

        # Get each data line
        # Add on quality to unpack string
        unpack_str += ('x'+str(num_quality_flags)+'s') * num_quality_words
        for line in handle:
          unpacked = struct.unpack(unpack_str, line.rstrip())

          # QUALT1 takes precedence
          quality_flags = unpacked[-num_quality_words:]

          # Build up the columns for the line
          flag_i = 0
          for i, parameter in enumerate(parameters):
              datum = float(unpacked[i])
              if datum is -9.0:
                  datum = float('nan')
              woce_flag = None
              # Only assign flag if column is flagged.
              if not asterisks[i].strip() == '':
                  woce_flag = int(quality_flags[0][flag_i])
                  flag_i += 1
              self.columns[parameter].set(i, datum, woce_flag)

        # Expand globals into columns
        #@header.each_pair do |header, value|
        #  column = @column_hash[header] = Column.new(header)
        #  column.values = Array.new(num_entries) {|i| value}


class DataFileCollection:

    def __init__(self, allow_contrived=False):
        self.files = []
        self.allow_contrived = allow_contrived

    def merge(datafile):
        raise NotImplementedError # TODO

    def split(self):
        raise NotImplementedError # TODO

    def stamps(self):
        return [file.stamp for file in self.files.values()]


# TODO Regions...maybe break this out into different parts of the library?

class Location:

    def __init__(self, coordinate, dtime=None, depth=None):
        self.coordinate = coordinate
        self.datetime = dtime
        self.depth = depth
        # TODO nil axis magnitudes should be matched as a wildcard


class Region:

    def __init__(self, name, *locations):
        self.name = name
        self.locations = locations

    def include (location):
        raise NotImplementedError # TODO

BASINS = REGIONS = {
    'Pacific': Region('Pacific', Location([1.111, 2.222]),
                      Location([-1.111, -2.222])),
    'East_Pacific': Region('East Pacific', Location([0, 0]), Location([1, 1]),
                           Location([3, 3]))
    # TODO define the rest of the basins...maybe define bounds for
    # other groupings
}


def identity_or_oob(x, oob=-999):
    """Identity or OOB (XXX)
       Args:
           x - anything
           oob - out-of-band value (default -999)
       Returns:
           identity or out-of-band value.
    """
    return x if x else oob
