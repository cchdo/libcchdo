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

from warnings import warn

import db.parameters
import fns

try:
    from math import isnan
except ImportError: # Cover when < python-2.6
    def isnan(n):
        return n != n


RADIUS_EARTH = 6371.01 #km

LIBVER = 'SIOCCHDLIB'

COLORS = {
    'RED': '\x1b\x5b1;31m',
    'YELLOW': '\x1b\x5b1;33m',
    'CYAN': '\x1b\x5b1;36m',
    'CLEAR': '\x1b\x5b0m',
}


class Parameter:

    PARAMETER_CACHE = {}

    def __init__(self, parameter_name, contrived=False):
        try:
            self.__dict__ = Parameter.PARAMETER_CACHE[parameter_name].__dict__
        except KeyError:
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
                #    db.parameters.init_from_postgresql(self, parameter_name)
                #except Exception, e:
                #    warn(("%s\nFalling back to mysql database for "
                #          "parameter info.") % e)
                try:
                    db.parameters.init_from_mysql(self, parameter_name)
                except Exception, e:
                    raise EnvironmentError(
                        ("%s\nNo databases could be used for "
                         "parameter verification.") % e)

            Parameter.PARAMETER_CACHE[parameter_name] = self

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
       return '%sColumn(%s): %s%s' % (COLORS['YELLOW'], self.parameter,
                                      COLORS['CLEAR'], self.values)

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
        try:
            return len(self.columns.values()[0])
        except:
            return 0


class DataFile:

    def __init__(self, allow_contrived=False):
        self.columns = {}
        self.stamp = None
        self.header = ''
        self.footer = None
        self.globals = {}
        self.allow_contrived = allow_contrived

    def expocodes(self):
        return fns.uniquify(self.columns['EXPOCODE'].values)

    def __len__(self):
        try:
            return len(self.columns.values()[0])
        except:
            return 0

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

    def __str__(self):
        s = ''
        s += '%sGlobals: %s\n' % (COLORS['RED'], COLORS['CLEAR'])
        for gv in self.globals.items():
            s += '%s: %s\n' % gv

        s += '%sData: %s\n' % (COLORS['RED'], COLORS['CLEAR'])
        for column in self.sorted_columns():
            s += str(column) + '\n'
            if column.is_flagged_woce():
                s += '\t%s%s%s\n' % (COLORS['CYAN'], column.flags_woce,
                                     COLORS['CLEAR'])
            if column.is_flagged_igoss():
                s += '\t%s%s%s\n' % (COLORS['CYAN'], column.flags_igoss,
                                     COLORS['CLEAR'])
        return s

    def to_hash(self):
        hash = {}
        for column in self.columns:
            c = self.columns[column]
            woce = c.parameter.woce_mnemonic 
            hash[woce] = c.values
            if c.is_flagged_woce():
                hash[woce+'_FLAG_W'] = c.flags_woce
            if c.is_flagged_igoss():
                hash[woce+'_FLAG_I'] = c.flags_igoss
        return hash

    # Refactored common code

    def create_columns(self, parameters, units=None):
        '''Create columns given parameters and their units.
           Args:
               parameters - parameters
               units - units to check. If None then no check is done.
        '''
        for i, parameter in enumerate(parameters):
            if parameter.endswith('FLAG_W') or \
               parameter.endswith('FLAG_I') or \
               parameter in self.columns:
                continue
            try:
                self.columns[parameter] = Column(parameter,
                                                 self.allow_contrived)
            except Exception, e:
                raise e

            if units:
                expected_units = \
                    self.columns[parameter].parameter.units_mnemonic
                given_unit = units[i]
                if expected_units != given_unit:
                    warn(("Mismatched units for %s. Expected '%s' and "
                          "received '%s'") % (parameter, expected_units,
                                              given_unit))


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
