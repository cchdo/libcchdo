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

COLOR_ESCAPE = '\x1b\x5b'
COLORS = {
    'RED': COLOR_ESCAPE + '1;31m',
    'YELLOW': COLOR_ESCAPE + '1;33m',
    'CYAN': COLOR_ESCAPE + '1;36m',
    'CLEAR': COLOR_ESCAPE + '0m',
}


class memoize:

    def __init__(self, callable):
        self._cache = {}
        self._callable = callable

    def __call__(self, *args, **kwargs):
        cache = self._cache
        key = kwargs and (args, hash(tuple(kwargs.items()))) or args
        try:
            return cache[key]
        except KeyError:
            value = cache[key] = self._callable(*args, **kwargs)
            return value


@memoize
class Parameter:
    ''' A CCHDO tracked parameter.
        
        The definition of the parameter is obtained from the CCHDO database
        using the given parameter name as the WOCE mnemonic to match.

        Any parameter_name with a leading '_' is contrived and is not
        searched for in the database. It is filled in with default values.
    '''

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

    def __eq__(self, other):
        return self.woce_mnemonic == other.woce_mnemonic

    def __str__(self):
        return 'Parameter %s' % self.woce_mnemonic


class Column:

   def __init__(self, parameter, contrived=False):
       if type(parameter) != str:
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


class File(object):

    def __init__(self):
        self.columns = {}
        self.header = ''

    def __len__(self):
        try:
            return len(self.columns.values()[0])
        except:
            return 0


class SummaryFile(File):
  
    def __init__(self):
        super(SummaryFile, self).__init__()
        columns = (
            "EXPOCODE SECT_ID STNNBR CASTNO DATE TIME LATITUDE LONGITUDE "
            "DEPTH _CAST_TYPE _CODE _NAV _WIRE_OUT _ABOVE_BOTTOM "
            "_MAX_PRESSURE _NUM_BOTTLES _PARAMETERS _COMMENTS").split()
        for column in columns:
            self.columns[column] = Column(column)

class DataFile(File):

    def __init__(self, allow_contrived=False):
        super(DataFile, self).__init__()
        self.stamp = None
        self.footer = None
        self.globals = {}
        self.allow_contrived = allow_contrived

    def expocodes(self):
        return fns.uniquify(self.columns['EXPOCODE'].values)

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


class DataFileCollection(object):

    def __init__(self, allow_contrived=False):
        self.files = []
        self.allow_contrived = allow_contrived

    def merge(datafile):
        raise NotImplementedError # TODO

    def split(self):
        raise NotImplementedError # TODO

    def stamps(self):
        return [file.stamp for file in self.files.values()]
