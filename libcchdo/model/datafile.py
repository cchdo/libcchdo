from operator import itemgetter

from libcchdo.fns import set_list, uniquify
from libcchdo.log import LOG
from libcchdo.ui import TERMCOLOR
from libcchdo.util import memoize
from libcchdo.db.model import std


class Column(object):

    def __init__(self, parameter, units=None):
        """Create a Column given a string parameter name or Parameter instance.

        """
        if not type(parameter) is str and not type(parameter) is unicode:
            self.parameter = parameter
        else:
            if type(parameter) is unicode:
                   parameter = parameter.encode('ascii', 'replace')
            self.parameter = std.make_contrived_parameter(parameter,
                                                          units=units)
        self.values = []
        self.flags_woce = []
        self.flags_igoss = []

    def get(self, index):
        if index >= len(self.values):
            return None
        return self.values[index]

    def set(self, index, value, flag_woce=None, flag_igoss=None):
        set_list(self.values, index, value)
        if flag_woce is not None:
            set_list(self.flags_woce, index, flag_woce)
        if flag_igoss is not None:
            set_list(self.flags_igoss, index, flag_igoss)

    def append(self, value=None, flag_woce=None, flag_igoss=None):
        self.values.append(value)
        i = len(self.values) - 1
        if flag_woce is not None:
            set_list(self.flags_woce, i, flag_woce)
        if flag_igoss is not None:
            set_list(self.flags_igoss, i, flag_igoss)

    def _check_range(self, value, flag_woce=None):
        """
        If a value is given, check that it is in range.
        If a flag is given with no value, set the flag.
        It is an error to set a flag for a missing value.
        If no flag is given, default to WOCE 2.

        """
        if value and not self.parameter.is_in_range(value):
            LOG.warn(
                u'{0!r} is not in range {1} ({2!r}, {3!r})'.format(
                    value, self.parameter, self.parameter.bound_lower,
                    self.parameter.bound_upper))
            flag_woce = 9
            value = None
        if value is None:
            if flag_woce is None:
                flag_woce = 9
            if flag_woce != 9:
                raise ValueError(
                    u'WOCE flag {0} set for missing data value'.format(
                    flag_woce))
        if value is not None and flag_woce is None:
            flag_woce = 2
        return value, flag_woce

    def set_check_range(self, index, value, flag_woce=None, flag_igoss=None):
        """Append while being concious of parameter ranges.

        """
        self.set(index, *self._check_range(value, flag_woce=flag_woce))

    def append_check_range(self, value, flag_woce=None):
        """Append while being concious of parameter ranges.

        """
        self.append(*self._check_range(value, flag_woce=flag_woce))

    def set_length(self, length, fill_value=None):
        """Set the length of the column and fill."""
        fill_length = length - len(self.values)
        self.values += [fill_value] * fill_length
        if self.flags_woce:
            self.flags_woce += [9] * fill_length
        if self.flags_igoss:
            self.flags_igoss += [9] * fill_length

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

    def is_global(self):
        """Return whether the values for the whole column are the same."""
        check = None
        for x in self.values:
            if check is None:
                check = x
                continue
            if check != x:
                return False
        return True

    def __str__(self):
        return '%sColumn(%s): %s%s' % (TERMCOLOR['YELLOW'], self.parameter,
                                       TERMCOLOR['CLEAR'], self.values)

    def __cmp__(self, other):
        try:
            return self.parameter.display_order - other.parameter.display_order
        except:
            return -1

    def check_and_replace_parameter(self, file, convert=True):
        parameter = self.parameter

        # A leading _ indicates a contrived parameter. Skip it.
        if parameter.name.startswith('_'):
            if '_FLAG_' in parameter.name:
                return
            LOG.info(
                u'Parameter {0!r} will not be checked against known '
                'parameters.'.format(parameter.name))
            return

        std_parameter = std.find_by_mnemonic(parameter.name)
        if not std_parameter:
            return

        if std_parameter.name != parameter.name:
            file[std_parameter.name] = file[parameter.name]
            del file[parameter.name]

        if not std_parameter:
            LOG.warn("Unknown parameter '%s'" % parameter.name)
            return

        given_units = parameter.units.mnemonic if parameter.units else None
        expected_units = std_parameter.units.mnemonic \
            if std_parameter and std_parameter.units else None
        from_to = (given_units, expected_units)

        if not convert:
            self.parameter = std_parameter
            if parameter.units and not parameter.units.id:
                units = std.Unit.find_by_name(parameter.units.name)
                if not units:
                    units = parameter.units
                if self.parameter.units and units.id != self.parameter.units.id:
                    # TODO figure out why setting without the id check causes
                    # "conflicting state already present in the identity map"
                    self.parameter.units = units
            else:
                self.parameter.units = None
            return

        if given_units and expected_units and \
           given_units != expected_units:
            LOG.warn(("Mismatched units for '%s'. Found '%s' but "
                      "expected '%s'") % ((parameter.name,) + from_to))
            # TODO IPTS-68 and ITS-90 need to be recognized and checked for as
            # valid aliases for DEG C. However, DEG C is less descriptive than
            # the given temperature specifications so retaining the original
            # specifications may be a good thing.
            try:
                unit_converter = file.unit_converters[from_to]
            except KeyError:
                LOG.info(("No unit converter registered with file for "
                          "'%s' -> '%s'. Skipping conversion.") % from_to)
                return
            LOG.info(("Converting from '%s' -> '%s' for %s.") % \
                     (from_to + (self.parameter.name,)))
            self = unit_converter(file, self)
            file.changes_to_report.append((
                'Converted %(parameter)s from %(startunit)s to %(endunit)s '
                'using %(technique)s') % {
                    'parameter': self.parameter.name,
                    'startunit': from_to[0],
                    'endunit': from_to[1],
                    'technique': file.unit_converter_technique.get(
                                     unit_converter, 'undescribed')
                })

        self.parameter = std_parameter


class File(object):

    def __init__(self):
        self.columns = {}
        self.unit_converters = {}
        self.unit_converter_technique = {}
        # Allow files to override column sorting by parameter display order.
        # Contains the columns in the order that they should appear.
        self.ordered_columns = []

        # Will report changes by inserting the stamp above the file's header
        # and prepending the changes. e.g.
        # # change[0]
        # # change[1]
        # # change[2]
        # # old stamp
        # # old header
        self.changes_to_report = []

    def sorted_columns(self):
        columns = self.columns.values()
        if self.ordered_columns:
            return filter(None,
                          [x for x in self.ordered_columns if x in columns])
        return sorted(columns)

    def get_property_for_columns(self, property_getter):
        return map(property_getter, self.sorted_columns())

    def column_headers(self):
        return self.get_property_for_columns(
            lambda column: column.parameter.mnemonic_woce())

    def __getitem__(self, index):
        return self.columns[index]

    def __setitem__(self, key, value):
        self.columns[key] = value

    def __delitem__(self, key):
        del self.columns[key]

    def __len__(self):
        try:
            return max(map(len, self.columns.values()))
        except:
            return 0

    def row(self, i):
        return [column[i] for column in self.sorted_columns()]

    def each_column(self, func, *args, **kwargs):
        for column in self.columns.values():
            func(column, self, *args, **kwargs)

    def check_and_replace_parameters(self, convert=True):
        self.each_column(Column.check_and_replace_parameter, convert=convert)


def station_equal(s0, s1):
    # TODO figure out how to compare station "numbers" reliably.
    if type(s0) is float:
        s0 = int(s0)
    if type(s1) is float:
        s1 = int(s1)
    return str(s0) == str(s1)


def cast_equal(c0, c1):
    # TODO figure out how to compare cast "numbers" reliably.
    if type(c0) is float:
        c0 = int(c0)
    if type(c1) is float:
        c1 = int(c1)
    return str(c0) == str(c1)


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

    def index(self, station, cast):
        for i, s in enumerate(self['STNNBR'].values):
            if not station_equal(s, station):
                continue
            if cast_equal(self['CASTNO'][i], cast):
                return i
        raise ValueError('%s, %s is not in summary file' % (station, cast))

    def __str__(self):
        s = u''
        s += '%sGlobals: %s\n' % (TERMCOLOR['RED'], TERMCOLOR['CLEAR'])
        for gv in self.globals.items():
            s += '%s: %s\n' % gv

        s += '%sData: %s\n' % (TERMCOLOR['RED'], TERMCOLOR['CLEAR'])
        for column in self.sorted_columns():
            s += '%s\n' % column
            if column.is_flagged_woce():
                s += '\t%s%s%s\n' % (TERMCOLOR['CYAN'], column.flags_woce,
                                     TERMCOLOR['CLEAR'])
            if column.is_flagged_igoss():
                s += '\t%s%s%s\n' % (TERMCOLOR['CYAN'], column.flags_igoss,
                                     TERMCOLOR['CLEAR'])
        return s.encode('ascii', 'replace')


class DataFile(File):
    PRESSURE_PARAMETERS = ('CTDPRS', 'CTDRAW', )

    def __init__(self, allow_contrived=False):
        super(DataFile, self).__init__()
        self.footer = None
        self.globals = {
            'stamp': '',
            'header': '',
        }
        self.allow_contrived = allow_contrived

    def expocodes(self):
        return uniquify(self['EXPOCODE'].values)

    def formats(self):
        return self.get_property_for_columns(
            lambda column: column.parameter.format)

    def parameters(self):
        return self.get_property_for_columns(lambda column: column.parameter)

    def parameter_mnemonics_woce(self):
        return self.get_property_for_columns(
            lambda column: column.parameter.mnemonic_woce() \
                           if column and column.parameter else '')

    def __copy__(self):
        """Effectively clones the structure of the DataFile
           Creates a new DataFile and creates new columns with the same
           parameters. A shallow copy of globals is made.
        """
        copy = DataFile()
        copy.create_columns(self.parameters())
        copy.globals = self.globals.copy()
        return copy

    def __str__(self):
        return unicode(self).encode('ascii', 'replace')

    def __unicode__(self):
        strs = []
        strs.append(u'%sGlobals: %s\n' % (TERMCOLOR['RED'], TERMCOLOR['CLEAR']))
        for gv in self.globals.items():
            strs.append(u'%s: %s\n' % gv)

        strs.append(u'%sData: %s\n' % (TERMCOLOR['RED'], TERMCOLOR['CLEAR']))
        for column in self.sorted_columns():
            strs.append(u'%s\n' % column)
            if column.is_flagged_woce():
                strs.append(u'\t%s%s%s\n' % (
                    TERMCOLOR['CYAN'], column.flags_woce, TERMCOLOR['CLEAR']))
            if column.is_flagged_igoss():
                strs.append(u'\t%s%s%s\n' % (
                    TERMCOLOR['CYAN'], column.flags_igoss, TERMCOLOR['CLEAR']))
        return u''.join(strs)

    def to_dict(self):
        d = {}
        for column in self.columns:
            c = self[column]
            woce = c.parameter.mnemonic_woce()
            d[woce] = c.values
            if c.is_flagged_woce():
                d[woce+'_FLAG_W'] = c.flags_woce
            if c.is_flagged_igoss():
                d[woce+'_FLAG_I'] = c.flags_igoss
        return d

    # Refactored common code

    def ensure_column(self, mnemonic):
        try:
            self[mnemonic]
        except KeyError:
            self[mnemonic] = Column(mnemonic)

    def create_columns(self, parameters, units=None, ordered=False):
        """Create columns given parameters and their units.
        Args:
            parameters - parameter names as WOCE mnemonics or Parameter
                instances
            units - units to check. If None then no check is done.
            ordered - specifies that the order the parameters were given is the
                order to use when columns are sorted

        """
        for i, parameter in enumerate(parameters):
            if isinstance(parameter, basestring):
                if (parameter.endswith('FLAG_W') or 
                    parameter.endswith('FLAG_I')):
                    LOG.info(
                        u'Skipped creating column for flag {0}'.format(
                        parameter))
                    continue
                elif parameter in self.columns:
                    LOG.info(
                        u'Skipped creating already present column {0}'.format(
                        parameter))
                    continue
                pname = parameter
            else:
                pname = parameter.mnemonic_woce()
            try:
                column = self[pname] = Column(
                    parameter, units[i] if units else None)
                if ordered:
                    self.ordered_columns.append(self[parameter])
            except Exception, e:
                raise e

            # Check the units
            if column.parameter and column.parameter.units:
                expected_units = column.parameter.units.mnemonic
            else:
                expected_units = None
            if units and expected_units:
                given_unit = units[i]
                if expected_units != given_unit:
                    LOG.warn(
                        u"Mismatched units for {0}. Expected {1!r} and "
                        "received {2!r}".format(
                        parameter, expected_units, given_unit))

    def swap_rows(self, a, b):
        """Swaps two rows in the file."""
        for c in self.columns.values():
            c.values[a], c.values[b] = c.values[b], c.values[a]
            if c.is_flagged_woce():
                c.flags_woce[a], c.flags_woce[b] = \
                    c.flags_woce[b], c.flags_woce[a]
            if c.is_flagged_igoss():
                c.flags_igoss[a], c.flags_igoss[b] = \
                    c.flags_igoss[b], c.flags_igoss[a]

    def sort_file_range(self, start, end, pres_ascending=True,
                        bot_ascending=False):
        """Sort the rows from indexes start to end by pressure and bottle."""
        pressure_col = None
        for p in self.PRESSURE_PARAMETERS:
            try:
                pressure_col = self.columns[p]
            except KeyError:
                pass
        if pressure_col is None:
            return

        bottle_col = None
        try:
            bottle_col = self['BTLNBR'].values
        except KeyError:
            bottle_col = [None] * len(pressure_col)

        pb_orders = zip(
            pressure_col.values[start:end], bottle_col[start:end],
            range(start, end))
        order = [i for p, b, i in pb_orders]
        # Sort first by bottle order
        reversed_pb_orders = sorted(
            pb_orders, key=itemgetter(1), reverse=(not bot_ascending))
        # Sort second by pressure
        sorted_pb_orders = sorted(
            reversed_pb_orders, key=itemgetter(0),
            reverse=(not pres_ascending))
        sorted_order = [i for p, b, i in sorted_pb_orders]

        # Don't keep swapping after just past the halfway point or things will
        # become out of order.
        for i in range(len(sorted_order) / 2 + 1):
            s = sorted_order[i]
            o = order[i]
            if s != o:
                k = order.index(s)
                self.swap_rows(s, o)
                order[i], order[k] = order[k], order[i]

    def reorder_file_pressure(self, pres_ascending=True, bot_ascending=False):
        """Reorders a file's rows by pressure then bottle number.

        This defaults to non-decreasing pressure and non-ascending bottle
        number order.

        """
        if len(self) > 0:
            stations = self['STNNBR'].values
            casts = self['CASTNO'].values
            station = stations[0]
            cast = casts[0]
            last_i = 0
            for i in range(1, len(self)):
                station_i = stations[i]
                cast_i = casts[i]
                if station_i != station or cast_i != cast:
                    station = station_i
                    cast = cast_i
                    self.sort_file_range(
                        last_i, i, pres_ascending, bot_ascending)
                    last_i = i
            self.sort_file_range(
                last_i, len(self), pres_ascending, bot_ascending)


class DataFileCollection(object):
    """Stores a collection of DataFiles

       A DataFileCollection represents data files that are actually a
       collection of many sub files (e.g. Exchange CTD files).
    """

    def __init__(self, allow_contrived=False):
        self.files = []
        self.allow_contrived = allow_contrived

    def stamps(self):
        return [file.globals['stamp'] for file in self.files.values()]

    def append(self, x):
        self.files.append(x)

    def to_data_file(self):
        df = DataFile()
        length = 0
        for file in self.files:
            num_rows = len(file)
            for row in range(num_rows):
                rowi = length + row
                # Insert globals
                for g, v in file.globals.items():
                    try:
                        df[g]
                    except KeyError:
                        df[g] = Column(g)
                        df[g].check_and_replace_parameter(df)
                    df[g].set(rowi, v)
                for c in file.sorted_columns():
                    mnemonic = c.parameter.mnemonic_woce()
                    try:
                        df[mnemonic]
                    except KeyError:
                        df[mnemonic] = Column(c.parameter)
                    try:
                        flag_woce = c.flags_woce[row]
                    except IndexError:
                        flag_woce = None
                    try:
                        flag_igoss = c.flags_igoss[row]
                    except IndexError:
                        flag_igoss = None
                    df[mnemonic].set(rowi, c[row], flag_woce, flag_igoss)
            length += num_rows
        return df

    def to_dict(self):
        d = {'files': []}
        for file in self.files:
            d['files'].append(file.to_dict())
        return d

    def __len__(self):
        return len(self.files)

    def get(self, index):
        if index >= len(self.files):
            return None
        return self.files[index]

    def __getitem__(self, key):
        return self.get(key)

    def __iter__(self):
        return self.files.__iter__()

    def __str__(self):
        s = u''
        for i, file in enumerate(self.files):
            s += '%sFILE %d %s\n' % (TERMCOLOR['RED'], i, TERMCOLOR['CLEAR'])
            s += str(file)
        return s.encode('ascii', 'replace')


