"""Merging DataFiles.

Merging is an integral part of maintaining a dataset. Mergers will receive
quality controlled, re-calibrated, etc. updates to parameter data up to years
after a cruise has taken place. These new data values or flags need to be
incorporated into what is currently available.

In order to merge data or flags into the current dataset, it is imperative to
determine where in the world each value was obtained. Only then can we match up
updated data with old data. This task is often accomplished by using EXPOCODE,
STNNBR, and CASTNO to identify a water column, then either using some
combination of BTLNBR and SAMPNO for bottles or some pressure parameter e.g.
CTDPRS or REVPRS for CTD to determine where in the vertical water column the
data belongs.

It is important to consider that flags may be merged separately from their
corresponding data values in the case of flag updates. Differentiating the
values and flags is done by referring to the value column as the parameter name,
and the flag columns as the parameter name with a '_FLAG_W' suffix for WOCE
style flags or '_FLAG_I' suffix for IGOSS style flags or another similar style.

"""
from copy import copy
from collections import OrderedDict

from pandas import *

pandas_merge = merge

from numpy import isnan, object as np_object

from libcchdo import LOG
from libcchdo.fns import is_list_globally
from libcchdo.formats import woce
from libcchdo.formats.exchange import (
    END_DATA, FILL_VALUE, FLAG_ENDING_WOCE, FLAG_ENDING_IGOSS)
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
            flag_name = param + FLAG_ENDING_WOCE
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
                    if param.endswith(FLAG_ENDING_WOCE):
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


def datafile_parameter_mnemonics(dfile):
    """Return a list of columns in a data file including flag columns."""
    columns = OrderedSet()
    for column in dfile.sorted_columns():
        mnemonic = column.parameter.mnemonic_woce()
        columns.add(mnemonic)
        if column.is_flagged_woce():
            columns.add(mnemonic + FLAG_ENDING_WOCE)
        if column.is_flagged_igoss():
            columns.add(mnemonic + FLAG_ENDING_IGOSS)
    return columns


def different_columns(origin, deriv):
    """Return different, not in origin, not in derivative, and common columns
    between two DataFiles.

    Columns include both value and flag columns.

    If arguments are DataFileCollections, return the columns that are different
    and missing for all the DataFiles as they would have been mapped for
    merging.

    """
    if type(origin) == DataFileCollection and type(deriv) == DataFileCollection:
        different = OrderedSet()
        not_in_origin = OrderedSet()
        not_in_derivative = OrderedSet()
        common = OrderedSet()

        dfile_map = map_collections(origin, deriv)
        for odfile, ddfile in dfile_map:
            diff, notino, notind, com = different_columns(odfile, ddfile)
            different |= diff
            not_in_origin |= notino
            not_in_derivative |= notind
            common |= com
        return (
            list(different), list(not_in_origin), list(not_in_derivative),
            list(common))

    origin_columns = datafile_parameter_mnemonics(origin)
    deriv_columns = datafile_parameter_mnemonics(deriv)

    common = origin_columns & deriv_columns
    not_in_origin = deriv_columns - origin_columns
    not_in_derivative = origin_columns - deriv_columns
    different = OrderedSet()

    # check common columns for differing data
    for col in common:
        if '_FLAG_' in col:
            param = col.split('_')[0]
        else:
            param = col
        origcol = origin[param]
        derivcol = deriv[param]
        diffcol = origcol.diff(derivcol)
        if not diffcol.is_diff():
            continue

        if not diffcol.is_diff_values() and not diffcol.is_diff_flags():
            different.add(col)
            common.remove(col)
            continue

        if 'FLAG' in col:
            if (    (col.endswith(FLAG_ENDING_WOCE) and
                     diffcol.is_diff_flags_woce()) or
                    (col.endswith(FLAG_ENDING_IGOSS) and
                     diffcol.is_diff_flags_igoss())):
                different.add(col)
                common.remove(col)
        elif diffcol.is_diff_values():
            different.add(col)
            common.remove(col)

    return (
        list(different), list(not_in_origin), list(not_in_derivative),
        list(common))


KEY_COLUMNS = [
    'EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO', 'SAMPNO', 'BTLNBR']


def overwrite_list(origin_col, derivative_col, keymap):
    """Return a copy of origin list overwritten by the derivative's values.

    The indices where the derivative values go in the origin column are
    given by keymap.

    """
    for ocoli, dcoli, key in keymap:
        try:
            dvvv = derivative_col[dcoli]
            try:
                origin_col[ocoli] = dvvv
            except IndexError:
                set_list(origin_col, ocoli, dvvv)
        except IndexError:
            pass
    return origin_col


def determine_params_to_merge(diffcols, not_in_orig_cols, not_in_deriv_cols,
                              commoncols, parameters):
    """Return the parameters to merge based on given and availability."""
    # make sure all given parameters to merge are actually different and exist
    parameters = OrderedSet(parameters)
    notdiff_params = parameters - diffcols - not_in_orig_cols - not_in_deriv_cols
    params_to_merge = parameters - notdiff_params
    if len(notdiff_params) != 0:
        unknown_parameters = []
        for param in notdiff_params:
            if param not in commoncols:
                unknown_parameters.append(param)
                notdiff_params.remove(param)
        if unknown_parameters:
            LOG.warn(
                u'Instructed to merge parameters that are not in either '
                'datafile: {0!r}'.format(list(unknown_parameters)))
        if notdiff_params:
            LOG.warn(u'Instructed to merge parameters that are not different: '
                     '{0!r}'.format(list(notdiff_params)))

    if not params_to_merge:
        raise ValueError(u'No columns selected to merge.')
    return params_to_merge


def map_keys(origin, deriv, keys):
    """Return a map of rows in origin to rows in deriv based on key columns."""
    # Map the deriv's rows onto the origin's rows based on the keys while
    # warning about missing keys in origin.
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

    # TODO similar to map_collections
    originkeys = OrderedDict()
    derivkeys = OrderedDict()
    for i in range(origin_collen):
        originkeys[tuple([col[i] for col in origincols])] = i
    for i in range(deriv_collen):
        derivkeys[tuple([col[i] for col in derivcols])] = i
    keymap = []
    for key in derivkeys:
        ideriv = derivkeys[key]
        try:
            iorigin = originkeys[key]
            keymap.append((iorigin, ideriv, key))
        except KeyError:
            LOG.warn(u'Key on row {0} of derivative file does not exist in '
                     'origin: {1!r}'.format(ideriv, key))
    for key in originkeys:
        iorigin = originkeys[key]
        if key in derivkeys:
            continue
        LOG.warn(u'Key on row {0} of origin file does not exist in derivative: '
                 '{1!r}'.format(iorigin, key))
    return keymap


def determine_ctd_keys(origin, deriv):
    # Determine key columns
    # In the case of CTD, the key column is the pressure column, whether it is
    # CTDPRS or REVPRS, or some other.
    # In the case of Bottle, the key column is the sample identifier which can
    # be made up of multiple columns e.g. EXPOCODE, STNNBR, CASTNO, BTLNBR,
    # SAMPNO
    # TODO It might be good for determination of the key to be overrideable by
    # the user...
    keycol = None
    for param in PRESSURE_PARAMETERS:
        if param in origin and param in deriv:
            keycol = param
            break
    if keycol is None:
        raise ValueError(u'No available pressure columns to merge CTD on.')
    LOG.info('Merging on pressure column: {0!r}'.format(keycol))
    return [keycol]


def merge_datafiles(origin, deriv, keys, parameters):
    """Merge the columns and data of two DataFiles."""
    diffcols, not_in_orig_cols, not_in_deriv_cols, commoncols = \
        different_columns(origin, deriv)
    params_to_merge = determine_params_to_merge(
        diffcols, not_in_orig_cols, not_in_deriv_cols, commoncols, parameters)

    param_keys = set(params_to_merge) & set(keys)
    if param_keys:
        raise ValueError(
            u'Cannot merge key column using itself: {0!r}'.format(param_keys))

    keymap = map_keys(origin, deriv, keys)

    # Create merged file using origin as template
    merged = copy(origin)

    # Create columns that are going to be added
    for param in not_in_orig_cols:
        if '_FLAG_' in param:
            continue
        merged[param] = Column(deriv[param].parameter)
        
    # There are two cases to consider when merging
    #
    # 1. New column is being added to original
    # 2. Column has been edited from original
    #
    # In both cases, the data values need to be inserted in the correct row
    # based on the key column values.
    # Additionally, it should be possible to specify whether only a flag column
    # gets merged or whether only column values get merged or which flag gets
    # merged. The way this could happen is...

    all_cols = commoncols + not_in_deriv_cols + keys + \
        list(OrderedSet(diffcols) | params_to_merge)
    for key in all_cols:
        if '_FLAG_' in key:
            param = key.split('_')[0]
        else:
            param = key
        if param in origin:
            col = merged[param]
            # copy the origin values in to be overwritten
            origincol = origin[param]
            if '_FLAG_' in key:
                if key.endswith(FLAG_ENDING_WOCE):
                    col.flags_woce = origincol.flags_woce
                elif key.endswith(FLAG_ENDING_IGOSS):
                    col.flags_igoss = origincol.flags_igoss
            else:
                col.values = origincol.values
    for key in params_to_merge:
        if '_FLAG_' in key:
            param = key.split('_')[0]
        else:
            param = key
        if param in deriv:
            col = merged[param]
            # For each param in deriv, update column with deriv value at origin
            # index
            derivcol = deriv[param]

            # Make sure the column is filled with fill values first.
            # set_length won't extend flag lists unless they evaluate to truthy
            if '_FLAG_' in key:
                if derivcol.flags_woce:
                    col.flags_woce = [9]
                    col.set_length(len(merged))
                    col.flags_woce = overwrite_list(
                        col.flags_woce, derivcol.flags_woce, keymap)
                if derivcol.flags_igoss:
                    col.flags_igoss = [9]
                    col.set_length(len(merged))
                    col.flags_igoss = overwrite_list(
                        col.flags_igoss, derivcol.flags_igoss, keymap)
            else:
                if derivcol.parameter.units:
                    try:
                        orig_units = col.parameter.units.name
                    except AttributeError:
                        orig_units = ''
                    try:
                        deriv_units = derivcol.parameter.units.name
                    except AttributeError:
                        deriv_units = ''
                    LOG.warn(u'Changed units for {0} from {1!r} to {2!r}'.format(
                        param, orig_units, deriv_units))
                    col.parameter.units = derivcol.parameter.units
                col.set_length(len(merged))
                col.values = overwrite_list(
                    col.values, derivcol.values, keymap)
    return merged


DATAFILE_KEYS = ['EXPOCODE', 'STNNBR', 'CASTNO']


def map_collections(origin, deriv, dfkeys=DATAFILE_KEYS):
    """Return list of tuples of matching DataFiles and the key.

    Match the datafiles based on keys composed of the given columns. 

    For files in origin not in derivative, a tuple with the origin DataFile
    as both parts of the tuple will be added.

    Warnings will be given for files in derivative but not in origin.

    """
    d_key_file = OrderedDict()
    o_key_file = OrderedDict()
    for ddfile in deriv:
        dfkey = tuple([ddfile.globals[key] for key in dfkeys])
        d_key_file[dfkey] = ddfile
    for odfile in origin:
        dfkey = tuple([odfile.globals[key] for key in dfkeys])
        o_key_file[dfkey] = odfile

    dfile_map = []
    for dfkey in o_key_file:
        odfile = o_key_file[dfkey]
        try:
            ddfile = d_key_file[dfkey]
            dfile_map.append((odfile, ddfile, dfkey))
        except KeyError:
            dfile_map.append((odfile, odfile, dfkey))
            LOG.warn(u'Origin file key {0!r} is not present in '
                     'derivative collection.'.format(dfkey))
    for dfkey in d_key_file:
        if dfkey in o_key_file:
            continue
        LOG.warn(u'Derivative file key {0!r} is not present in '
                 'origin collection.'.format(dfkey))
    return dfile_map


def merge_collections(origin, deriv, merge, dfkeys=DATAFILE_KEYS):
    """Match up files in two archives and apply the merge function to them."""
    # Only merge files into the ones already present in origin. Warn if any
    # files from deriv are not used
    merged_dfc = DataFileCollection()
    dfile_map = map_collections(origin, deriv)
    for odfile, ddfile, dfkey in dfile_map:
        LOG.info(u'Merging files for key {0}'.format(dfkey))
        merged_dfc.append(merge(odfile, ddfile))
    return merged_dfc
