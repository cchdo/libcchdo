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

import os
import logging

try:
    from math import isnan
except ImportError:
    # Just in case python < 2.6
    def isnan(n):
        return n != n


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


_LIBLOG_HANDLER = logging.StreamHandler()
_LIBLOG_HANDLER.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

LOG = logging.getLogger('libcchdo')
LOG.setLevel(logging.DEBUG)
LOG.addHandler(_LIBLOG_HANDLER)


class memoize(object):

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


def get_library_abspath():
    import os
    import os.path as path
    import inspect
    return os.path.split(os.path.abspath(inspect.getfile(
                       inspect.currentframe())))[0]


def set_list(L, i, value, fill=None):
    try:
        L[i] = value
    except IndexError:
        L.extend([fill] * (i - len(L) + 1))
        L[i] = value


import db
import db.parameters
import fns
import formats


RADIUS_EARTH = 6371.01 #km

LIBVER = 'SIOCCHDLIB'

COLOR_ESCAPE = '\x1b\x5b'
COLORS = {
    'RED': COLOR_ESCAPE + '1;31m',
    'YELLOW': COLOR_ESCAPE + '1;33m',
    'CYAN': COLOR_ESCAPE + '1;36m',
    'CLEAR': COLOR_ESCAPE + '0m',
}


class Column(object):

   def __init__(self, parameter, units=None):
       if type(parameter) != str:
           self.parameter = parameter
       else:
           self.parameter = db.parameters.make_contrived_parameter(
                                parameter, units=units)
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
       set_list(self.values, index, value)
       if flag_woce is not None:
           set_list(self.flags_woce, index, flag_woce)
       if flag_igoss is not None:
           set_list(self.flags_igoss, index, flag_igoss)

   def append(self, value=None, flag_woce=None, flag_igoss=None):
       self.values.append(value)
       i = len(self.values) - 1
       if flag_woce is not None:
           self.flags_woce.insert(i, flag_woce)
       if flag_igoss is not None:
           self.flags_igoss.insert(i, flag_igoss)

   def __getitem__(self, key):
       return self.get(key)

   def __setitem__(self, key, value):
       self.set(key, value)

   def __iter__(self):
       return self.values.__iter__()

   def __contains__(self, v):
       return v in self.values

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
       try:
           return self.parameter.display_order - other.parameter.display_order
       except:
           return -1


class File(object):

    def __init__(self):
        self.columns = {}
        self.unit_converters = {}

    def sorted_columns(self):
        return sorted(self.columns.values())

    def get_property_for_columns(self, property_getter):
        return map(property_getter, self.sorted_columns())

    def __getitem__(self, index):
        return self.columns[index]

    def __setitem__(self, key, value):
        self.columns[key] = value

    def __len__(self):
        try:
            return len(self.columns.values()[0])
        except:
            return 0

    def check_and_replace_parameters(self):
        for column in self.columns.values():
            parameter = column.parameter
            std_parameter = db.parameters.find_by_mnemonic_std(parameter.name)
            
            if parameter.name.startswith('_'):
                continue

            if not std_parameter:
                LOG.warn("Unknown parameter '%s'" % parameter.name)
                continue

            given_units = parameter.units.mnemonic if parameter.units else None
            expected_units = std_parameter.units.mnemonic \
                if std_parameter and std_parameter.units else None
            from_to = (given_units, expected_units)

            if given_units and expected_units and \
               given_units != expected_units:
                LOG.warn(("Mismatched units for '%s'. Found '%s' but "
                          "expected '%s'") % ((parameter.name,) + from_to))
                try:
                    unit_converter = self.unit_converters[from_to]
                    LOG.info(("Converting from '%s' -> '%s' for %s.") % \
                         (from_to + (column.parameter.name,)))
                    column = unit_converter(self, column)
                except KeyError:
                    LOG.info(("No unit converter registered with file for "
                                  "'%s' -> '%s'. Skipping conversion.") % \
                                  from_to)
                    continue

            column.parameter = std_parameter


class SummaryFile(File):
  
    def __init__(self):
        super(SummaryFile, self).__init__()
        self.globals = {
            'stamp': '',
            'header': '',
        }
        columns = (
            "EXPOCODE SECT_ID STNNBR CASTNO DATE TIME LATITUDE LONGITUDE "
            "DEPTH _CAST_TYPE _CODE _NAV _WIRE_OUT _ABOVE_BOTTOM "
            "_MAX_PRESSURE _NUM_BOTTLES _PARAMETERS _COMMENTS").split()
        for column in columns:
            self[column] = Column(column)


class DataFile(File):

    def __init__(self, allow_contrived=False):
        super(DataFile, self).__init__()
        self.footer = None
        self.globals = {
            'stamp': '',
            'header': '',
        }
        self.allow_contrived = allow_contrived

    def expocodes(self):
        return fns.uniquify(self['EXPOCODE'].values)

    def column_headers(self):
        return self.get_property_for_columns(
            lambda column: column.parameter.mnemonic_woce())

    def formats(self):
        return self.get_property_for_columns(
            lambda column: column.parameter.format)

    def __str__(self):
        s = u''
        s += '%sGlobals: %s\n' % (COLORS['RED'], COLORS['CLEAR'])
        for gv in self.globals.items():
            s += '%s: %s\n' % gv

        s += '%sData: %s\n' % (COLORS['RED'], COLORS['CLEAR'])
        for column in self.sorted_columns():
            s += '%s\n' % column
            if column.is_flagged_woce():
                s += '\t%s%s%s\n' % (COLORS['CYAN'], column.flags_woce,
                                     COLORS['CLEAR'])
            if column.is_flagged_igoss():
                s += '\t%s%s%s\n' % (COLORS['CYAN'], column.flags_igoss,
                                     COLORS['CLEAR'])
        return s.encode('ascii', 'replace')

    def to_hash(self):
        hash = {}
        for column in self.columns:
            c = self[column]
            woce = c.parameter.mnemonic_woce()
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
               parameters - parameter names as WOCE mnemonics
               units - units to check. If None then no check is done.
        '''
        for i, parameter in enumerate(parameters):
            if parameter.endswith('FLAG_W') or \
               parameter.endswith('FLAG_I') or \
               parameter in self.columns:
                continue
            try:
                self[parameter] = Column(
                    parameter, units[i] if units else None)
            except Exception, e:
                raise e

            column = self[parameter]
            expected_units = \
                column.parameter.units.mnemonic if column.parameter and \
                column.parameter.units else None
            if units and expected_units:
                given_unit = units[i]
                if expected_units != given_unit:
                    LOG.warn(("Mismatched units for %s. Expected '%s' and "
                              "received '%s'") % \
                             (parameter, expected_units, given_unit))


class DataFileCollection(object):

    def __init__(self, allow_contrived=False):
        self.files = []
        self.allow_contrived = allow_contrived

    def stamps(self):
        return [file.globals['stamp'] for file in self.files.values()]

    def __str__(self):
        s = u''
        for i, file in enumerate(self.files):
            s += '%sFILE %d %s\n' % (COLORS['RED'], i, COLORS['CLEAR'])
            s += str(file)
        return s.encode('ascii', 'replace')


# Initialize the database file, if it is not present.

library_db_file_path = os.path.join(get_library_abspath(), 
    'db', db.connect._DB_LIBRARY_FILE)

if not os.path.isfile(library_db_file_path):
    LOG.info(
        "The library's missing database file (%s) was auto-generated." % \
        library_db_file_path)
    import db.model.std
    import db.model.convert as convert
    db.model.std.create_all()

    std_session = db.model.std.session()
    std_session.add_all(convert.all_parameters())
    std_session.commit()
    std_session.close()
