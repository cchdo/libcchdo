from copy import copy

from pandas import *

pandas_merge = merge

from numpy import isnan, object as np_object

from libcchdo import LOG
from libcchdo.formats import woce
from libcchdo.formats.exchange import END_DATA, FILL_VALUE, FLAG_WOCE_ENDING
from libcchdo.fns import equal_with_epsilon, set_list
from libcchdo.recipes.orderedset import OrderedSet
from libcchdo.model.datafile import (
    DataFile, DataFileCollection, Column, PRESSURE_PARAMETERS)


KEY_COLS = ['EXPOCODE', 'STNNBR', 'CASTNO', 'SAMPNO', 'BTLNBR']


class MergeData(object):
    def __init__(self, stamp, header, param_units, dframe):
        self.stamp = stamp
        self.header = header
        self.param_units = param_units
        self.dframe = dframe

    def available_keys(self):
        """Return keys that are available to merge on."""
        df_cols = list(self.dframe.columns)
        return [col for col in KEY_COLS if col in df_cols]

    def grouped(self, keys=None):
        if keys is None:
            keys = self.available_keys()
        return self.dframe.groupby(keys, axis=0)

    def merge_keys(self, other):
        """Return the keys that are in both mergedatas."""
        keys1 = self.available_keys()
        keys2 = other.available_keys()
        if keys1 != keys2:
            LOG.warn(u'Mismatched key composition to merge on:\norigin:\t\t{0!r}\n'
                      'derivative:\t{1!r}'.format(keys1, keys2))
            LOG.warn(u'Merging on common subset.')
            on_cols = list(OrderedSet(keys1) & OrderedSet(keys2))
        else:
            on_cols = keys1

        LOG.info('Merging using keys composed of: {0!r}'.format(on_cols))
        return on_cols

    def map_rows(self, other, on_cols=None):
        """Return a map of the rows in mergedata1 to mergedata2."""
        if on_cols is None:
            on_cols = self.merge_keys(other)
        df2_grouped = other.grouped(on_cols)
        df1_grouped = self.grouped(on_cols)
        df2_groups = df2_grouped.groups
        df1_groups = df1_grouped.groups
        df1_ids = df1_groups.keys()

        row_map = []
        for cast_identifier, group in df2_grouped:
            if cast_identifier not in df1_ids:
                LOG.warn(
                    u'Key {0} in derivative file is not in origin file'.format(
                    cast_identifier))
                continue
            # Find the rows that correspond to the same data row
            row1 = df1_groups[cast_identifier][0]
            row2 = df2_groups[cast_identifier][0]
            row_map.append([row1, row2, cast_identifier])
        return row_map

    def convert_to_datafile(self, parameters):
        dfile = DataFile()

        header = '# Merged parameters: {0}\n#{1}\n{2}\n'.format(
            ', '.join(parameters), self.stamp, '\n'.join(self.header))
        dfile.globals['header'] = header

        params = self.dframe.head(1)
        units = []
        for param in params:
            units.append(self.param_units[param])
        dfile.create_columns(params, units)
        dfile.check_and_replace_parameters()

        for param in params:
            if 'FLAG' in param:
                continue
            dfile[param].values = list(self.dframe[param])
            flag_name = param + FLAG_WOCE_ENDING
            if flag_name in params:
                dfile[param].flags_woce = list(self.dframe[flag_name])

        woce.fuse_datetime(dfile)
        return dfile


class Merger(object):
    def __init__(self, file1, file2):
        self.mdata1 = self.read_file(file1)
        self.mdata2 = self.read_file(file2)

    def read_file(self, fobj):
        # TODO this should really be a read into libcchdo.model.datafile format
        # and then converted into a pandas dataframe, or not at all because of
        # precision problems.
        stamp, header, param_units, last_line = self._read_head_foot(fobj)
        fobj.seek(0)

        # Skip the stamp, headers and units line when reading the CSV as a
        # dataframe
        header_len = len(header)
        skiprows = [header_len, header_len + 2]
        if last_line:
            skip_footer = 1
        else:
            skip_footer = 0

        dframe = read_csv(fobj, header=(header_len), skiprows=skiprows,
                          skip_footer=skip_footer)

        # For columns with string values, strip whitespace
        for param in dframe.head(1):
            if dframe[param].dtype == np_object:
                dframe[param] = [xxx.strip() for xxx in dframe[param]]

        return MergeData(stamp, header, param_units, dframe)

    def _read_head_foot(self, fobj):
        header = []
        stamp = fobj.readline().rstrip()
        if not stamp.startswith('BOTTLE') and not stamp.startswith('CTD'):
            raise ValueError(
                'Stamp {0!r} must start with BOTTLE or CTD'.format(stamp))
        line = fobj.readline()
        while line:
            if not line.startswith('#'):
                break
            header.append(line.rstrip())
            line = fobj.readline()
        params = line.rstrip().split(',')
        line = fobj.readline()
        units = line.rstrip().split(',')
        param_units = dict(zip(params, units))
        lines = fobj.readlines()
        last_line = lines[-1].startswith(END_DATA)
        return stamp, header, param_units, last_line

    def map_rows(self, on_cols=None):
        return self.mdata1.map_rows(self.mdata2, on_cols)

    def different_cols(self):
        different_cols = OrderedSet()
        dframe2 = self.mdata2.dframe
        dframe1 = self.mdata1.dframe
        columns2 = dframe2.columns
        columns1 = dframe1.columns
        row_map = self.map_rows()
        for col in columns2:
            if col not in columns1:
                different_cols.add(col)
                continue

            for row1, row2, cast_identifier in row_map:
                # Make sure the values for both dataframes matches
                val1 = dframe1[col][row1]
                val2 = dframe2[col][row2]
                try:
                    float(val1)
                    float(val2)
                    if val1 != val2 and not equal_with_epsilon(val1, val2):
                        LOG.info(u'{0} differs at {1}:\t{2!r} {3!r}'.format(col,
                            cast_identifier, val1, val2))
                        different_cols.add(col)
                except ValueError:
                    if val1.strip() != val2.strip():
                        different_cols.add(col)
        return list(different_cols)
        
    def merge(self, columns_to_merge):
        df1 = self.mdata1.dframe
        df2 = self.mdata2.dframe
        df1_cols = list(df1.columns)
        df2_cols = list(df2.columns)

        on_cols = self.mdata1.merge_keys(self.mdata2)
        row_map = self.map_rows(on_cols)

        if not row_map:
            LOG.error(u'No keys matched in origin and derivative files.')
            return None

        merged_df = df1.copy(deep=True)

        # TODO consideration: DataFrame does not handle precision.
        for col in columns_to_merge:
            # Case 1: Adding new column
            if col not in df1_cols:
                if col not in df2_cols:
                    LOG.warn(u'No such column: {0!r}'.format(col))
                    continue
                LOG.info(u'Adding new column {0!r}'.format(col))
                # Make temporary frame with only the data and keys to merge
                temp_frame = df2.copy(deep=True)
                for col_to_check in df2.columns:
                    if col_to_check not in on_cols + [col]:
                        del temp_frame[col_to_check]

                merged_df = pandas_merge(
                    merged_df, temp_frame, how='outer', on=on_cols)
                # If the new file has less records, fill in the missing records
                fill_map = {}
                for param in merged_df.head(1):
                    if param.endswith(FLAG_WOCE_ENDING):
                        fill_map[param] = 9
                    else:
                        fill_map[param] = FILL_VALUE
                merged_df = merged_df.fillna(fill_map)
            # Case 2: Updating column data
            else:
                # WARNING: using pandas.DataFrame.update or join causes the
                # dtypes for columns to change. This is highly undesirable, do
                # not use them.
                LOG.info(u'Updating data for column {0!r}'.format(col))
                col1 = df1[col]
                col2 = df2[col]
                colm = merged_df[col]
                for row1, row2, cast_identifier in row_map:
                    val1 = col1[row1]
                    val2 = col2[row2]
                    colm[row1] = val2

        # Overwrite parameter units from origin with deriv
        param_units = dict(**self.mdata1.param_units)
        param_units.update(self.mdata2.param_units)
        return MergeData(
            self.mdata1.stamp, self.mdata1.header, param_units, merged_df)


def merge_ctd_bacp_xmiss_and_ctd_exchange(file, mergefile):
    """Merge mergefile onto file"""
    merge_pressure = None
    pressure = None
    for c in PRESSURE_PARAMETERS:
        try:
            merge_pressure = mergefile[c]
            pressure = file[c]
        except KeyError:
            pass
    if merge_pressure is None or pressure is None:
        LOG.warn(
            'Unable to find a matching pressure column in both files. Could '
            'not merge.')
        return 1

    param = 'XMISS'
    param = 'TRANSM'

    xmiss_column = None
    try:
        xmiss_column = file['TRANSM']
    except KeyError:
        pass
    if not xmiss_column:
        xmiss_column = file['TRANSM'] = Column('TRANSM')
        xmiss_column.values = [None] * len(file)

    merge_xmiss = None
    try:
        merge_xmiss = mergefile['TRANSM']
    except KeyError:
        pass
    if not merge_xmiss:
        LOG.warn('Merge file has no {0} column to merge'.format(param))
        return 1

    for i, p in enumerate(merge_pressure.values):
        j = pressure.values.index(p)
        xmiss_column.values[j] = merge_xmiss.values[i]


def different_columns(origin, deriv):
    """Return the columns that are different and missing between two DataFiles.

    """
    origin_columns = OrderedSet(origin.parameter_mnemonics_woce())
    deriv_columns = OrderedSet(deriv.parameter_mnemonics_woce())

    common = origin_columns & deriv_columns
    missing = origin_columns ^ deriv_columns
    different = OrderedSet()

    # check common columns for differing data
    for col in common:
        origcol = origin[col]
        derivcol = deriv[col]
        diffcol = origcol.diff(derivcol)
        if diffcol:
            different.add(col)
    return list(different), list(missing)


def obtain_merged_columns(origin, deriv, mergeable_columns):
    """Ask the user to select the merged colums."""
    merged_columns = []
    deleted_columns = []

    origin_columns = origin.parameter_mnemonics_woce()
    deriv_columns = deriv.parameter_mnemonics_woce()

    for column in mergeable_columns:
        if column in origin_columns and column not in deriv_columns:
            print ('The column "%s" is in the original file, but not in the '
                    'merge file.' % column)
            if 'n' in raw_input('Keep it? [Y/n] ').lower():
                LOG.warn('Withholding original column "%s" from output'
                        % column)
                deleted_columns.append(column)
            else:
                LOG.info('Keeping "%s" in output' % column)
                merged_columns.append(column)
        elif column not in origin_columns and column in deriv_columns:
            print 'The column "%s" is new in the merge file.' % column
            if 'n' in raw_input('Add it? [Y/n] ').lower():
                LOG.info('Withholding merge column "%s" from output'
                        % column)
            else:
                LOG.warn('Merging "%s" into output' % column)
                merged_columns.append(column)
        elif column in origin_columns and column in deriv_columns:
            if origin[column] == deriv[column]:
                LOG.info('The column "%s" is unchanged between original and merge files.' % column)
                continue
            LOG.warn('The column "%s" appears in both files.' % column)
            LOG.warn('    Original: %s' %
                    repr(origin[column].values[:10]))
            LOG.warn('    Derivative: %s' %
                    repr(deriv[column].values[:10]))
            action = raw_input('What shall I do (keep, merge, abort)? [k/m/A] ').lower()
            if 'k' in action:
                LOG.warn('Keeping original "%s" in output' % column)


KEY_COLUMNS = [
    'EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO', 'SAMPNO', 'BTLNBR']


def key_columns(dfile, possible_keys=KEY_COLUMNS):
    """Return the columns of the DataFile that comprise the key."""
    return [col for col in dfile.parameter_mnemonics_woce()
            if col in possible_keys]


def nonkey_columns(dfile, possible_keys=KEY_COLUMNS):
    """Return the columns of the DataFile that do not comprise the key."""
    return [col for col in dfile.parameter_mnemonics_woce()
            if col not in possible_keys]


def merge_data(origin, deriv, keys, parameters):
    """Merge the columns and data of two CTD files."""
    diffcols, missingcols = different_columns(origin, deriv)

    # make sure all given parameters to merge are actually different
    notdiff_params = OrderedSet(parameters) - diffcols - missingcols
    if len(notdiff_params) != 0:
        LOG.warn(u'Instructed to merge parameters that are not different: '
                  '{0!r}'.format(list(notdiff_params)))

    param_keys = set(parameters) & set(keys)
    if param_keys:
        raise ValueError(
            u'Cannot merge key column using itself: {0!r}'.format(param_keys))

    # There are two cases to consider when merging
    #
    # 1. New column is being added to original
    # 2. Column has been edited from original
    #
    # In both cases, the data values need to be inserted in the correct row
    # based on the key column values.
    #
    # Determining key columns
    # =======================
    #
    # In the case of CTD, the key column is the pressure column.
    # In the case of Bottle, the key column is the sample identifier which can
    # be made up of multiple columns e.g. EXPOCODE, STNNBR, CASTNO, BTLNBR,
    # SAMPNO
    # TODO It might be good for determination of the key to be overrideable by
    # the user...

    # determine key columns
    # This is currently only a CTD merge, so find the first pressure parameter
    # available.
    key = None
    for param in PRESSURE_PARAMETERS:
        if param in origin.columns and param in deriv.columns:
            key = param
            break
    if key is None:
        raise ValueError(u'No available pressure columns to merge CTD on.')
    LOG.info('Merging on pressure column: {0!r}'.format(key))
    keys = [key]

    # Map the deriv's rows onto the origin's rows based on the keys while
    # warning about missing keys in origin.
    keymap = {}
    origincols = [origin[param] for param in keys]
    derivcols = [deriv[param] for param in keys]

    # Make sure all key columns are equal length
    origin_collens = [len(col) for col in origincols]
    origin_collen = origin_collens[0]
    deriv_collens = [len(col) for col in derivcols]
    deriv_collen = deriv_collens[0]
    if sum(deriv_collens) / len(derivcols) != deriv_collen:
        raise ValueError(u'Key columns are of differing lengths: {0!r}'.format(
            zip(derivcols, collens)))

    # collect keys for both sides
    originkeys = []
    derivkeys = []
    for i in range(origin_collen):
        originkeys.append(tuple([col[i] for col in origincols]))
    for i in range(deriv_collen):
        derivkeys.append(tuple([col[i] for col in derivcols]))

    for ideriv, key in enumerate(derivkeys):
        try:
            iorigin = originkeys.index(key)
            keymap[ideriv] = iorigin
        except ValueError:
            LOG.warn(u'Key on row {0} of derivative file does not exist in '
                     'origin: {1!r}'.format(ideriv, key))

    # Create merged file using origin as template
    merged = copy(origin)

    # Create columns that are going to be added
    for param in missingcols:
        merged[param] = Column(deriv[param].parameter)

    # For each column, run down the rows and copy in the values and flags.
    for key in merged.columns:
        col = merged[key]
        if key in origin.columns:
            # copy the origin values in to be overwritten
            origincol = origin[key]
            col.values = origincol.values
            col.flags_woce = origincol.flags_woce
            col.flags_igoss = origincol.flags_igoss
        if key in deriv.columns:
            # For each key in deriv, update column with deriv value at origin
            # index
            for i in range(len(derivkeys)):
                derivcol = deriv[key]
                try:
                    coli = keymap[i]
                except KeyError:
                    continue
                try:
                    dval = derivcol.values[i]
                    try:
                        col.values[coli] = dval
                    except IndexError:
                        col.set(coli, dval)
                except IndexError:
                    pass
                try:
                    dflag_woce = derivcol.flags_woce[i]
                    try:
                        col.flags_woce[coli] = dflag_woce
                    except IndexError:
                        set_list(col.flags_woce, coli, dflag_woce)
                except IndexError:
                    pass
                try:
                    dflag_igoss = derivcol.flags_igoss[i]
                    try:
                        col.flags_igoss[coli] = dflag_igoss
                    except IndexError:
                        set_list(col.flags_woce, coli, dflag_woce)
                except IndexError:
                    pass
    return merged


def merge_archives(origin, deriv, merge,
                   dfkeys=['EXPOCODE', 'STNNBR', 'CASTNO']):
    """Match up files in two archives and apply the merge function to them."""
    # Only merge files into the ones already present in origin. Warn if any
    # files from deriv are not used
    merged_dfc = DataFileCollection()
    for ddfile in deriv.files:
        dfkey = tuple([ddfile.globals[key] for key in dfkeys])
        merged = False
        for odfile in origin.files:
            ofkey = tuple([odfile.globals[key] for key in dfkeys])
            if ofkey == dfkey:
                LOG.debug('merging archives on file key {0}'.format(ofkey))
                merged_dfc.append(merge(odfile, ddfile))
                merged = True
                break
        if not merged:
            LOG.warn(u'Derivative file key {0!r} is not present in '
                     'origin.'.format(dfkey))
    return merged_dfc
