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

from libcchdo import LOG
from libcchdo.formats import woce
from libcchdo.formats.exchange import (
    END_DATA, FILL_VALUE, FLAG_ENDING_WOCE, FLAG_ENDING_IGOSS)
from libcchdo.fns import equal_with_epsilon, set_list
from libcchdo.recipes.orderedset import OrderedSet
from libcchdo.model.datafile import (
    DataFile, DataFileCollection, Column, PRESSURE_PARAMETERS)


BOTTLE_KEY_COLS = ('EXPOCODE', 'STNNBR', 'CASTNO', 'SAMPNO', 'BTLNBR',)


DFILE_KEY_COLS = ('EXPOCODE', 'STNNBR', 'CASTNO',)


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


def _datafile_parameter_mnemonics(dfile):
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


def different_columns(origin, deriv, keys, row_map=None):
    """Return different, not in origin, not in derivative, and common columns
    between two DataFiles.

    Columns include both value and flag columns.

    If arguments are DataFileCollections, return the columns that are different
    and missing for all the DataFiles as they would have been mapped for
    merging.

    """
    if not row_map:
        row_map = map_keys(origin, deriv, keys)
    if type(origin) == DataFileCollection and type(deriv) == DataFileCollection:
        different = OrderedSet()
        not_in_origin = OrderedSet()
        not_in_derivative = OrderedSet()
        common = OrderedSet()

        dfile_map = map_collections(origin, deriv)
        for odfile, ddfile, dfkey in dfile_map:
            diff, notino, notind, com = different_columns(
                odfile, ddfile, [], row_map=row_map)
            different |= diff
            not_in_origin |= notino
            not_in_derivative |= notind
            common |= com
        return (
            list(different), list(not_in_origin), list(not_in_derivative),
            list(common))

    origin_columns = _datafile_parameter_mnemonics(origin)
    deriv_columns = _datafile_parameter_mnemonics(deriv)

    common = origin_columns & deriv_columns
    not_in_origin = deriv_columns - origin_columns
    not_in_derivative = origin_columns - deriv_columns
    different = OrderedSet()

    def report_differences(param, difflist, difftuples):
        for i, diff in enumerate(difflist):
            if not diff:
                continue
            LOG.info(u'{0} differs at origin row {1}:\t{2!r}'.format(
                param, i, difftuples[i]))
        

    # check common columns for differing data
    for col in common:
        if '_FLAG_' in col:
            param = col.split('_')[0]
        else:
            param = col
        origcol = origin[param]
        derivcol = deriv[param]
        diffcol = origcol.diff(derivcol, row_map=row_map)
        if not diffcol.is_diff():
            continue

        is_diff = False
        if 'FLAG' in col:
            if (    (col.endswith(FLAG_ENDING_WOCE) and
                     diffcol.is_diff_flags_woce()) or
                    (col.endswith(FLAG_ENDING_IGOSS) and
                     diffcol.is_diff_flags_igoss())):
                is_diff = True

            # Report where the flags first differ.
            if col.endswith(FLAG_ENDING_WOCE):
                report_differences(
                    col, diffcol.flags_woce, diffcol.flags_woce_tuples)
            elif col.endswith(FLAG_ENDING_IGOSS):
                report_differences(
                    col, diffcol.flags_igoss, diffcol.flags_igoss_tuples)
        else:
            if diffcol.is_diff_values():
                is_diff = True

                report_differences(
                    col, diffcol.values, diffcol.values_tuples)
            elif diffcol.is_diff_units():
                is_diff = True
            elif diffcol.is_diff_flags():
                pass
            elif diffcol.is_diff_length():
                pass
            else:
                raise ValueError(u'{0} columns differ in a way that cannot be '
                                 'merged: {1}'.format(col, diffcol))
        if is_diff:
            different.add(col)
            common.remove(col)
    return (
        list(different), list(not_in_origin), list(not_in_derivative),
        list(common))


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


def filter_params_to_merge(diffcols, not_in_orig_cols, not_in_deriv_cols,
                           commoncols, parameters_to_merge):
    """Filter the parameters_to_merge based on given and availability."""
    # make sure all given parameters to merge are actually different and exist
    parameters_to_merge = OrderedSet(parameters_to_merge)
    notdiff_params = \
        parameters_to_merge - diffcols - not_in_orig_cols - not_in_deriv_cols
    params_to_merge = parameters_to_merge - notdiff_params
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
        raise ValueError(u'No columns selected to merge are different.')
    LOG.info(u'Merging {0}'.format(list(params_to_merge)))
    return params_to_merge


def map_keys(origin, deriv, keys):
    """Return a map of rows in origin to rows in deriv based on key columns."""
    if not keys:
        LOG.error(u'No keys provided to map on.')
        return []

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
        key = tuple([col[i] for col in origincols])
        originkeys[key] = i
    for i in range(deriv_collen):
        key = tuple([col[i] for col in derivcols])
        derivkeys[key] = i

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

    if not keymap:
        LOG.error(u'No keys matched in origin and derivative files.')

    return keymap


def _determine_available_bottle_keys(dfile):
    """Return keys that are available to merge on."""
    df_cols = _datafile_parameter_mnemonics(dfile)
    return tuple([col for col in BOTTLE_KEY_COLS if col in df_cols])


def determine_bottle_keys(origin, deriv):
    """Return the columns that should compose the key to merge on.

    These are columns that are available in both files to merge.

    The keys are the sample identifier which can be made up of multiple
    columns e.g. EXPOCODE, STNNBR, CASTNO, BTLNBR,
    SAMPNO

    """
    keys1 = _determine_available_bottle_keys(origin)
    keys2 = _determine_available_bottle_keys(deriv)
    if keys1 != keys2:
        LOG.warn(u'Mismatched key composition to merge on:\norigin:\t\t{0!r}\n'
                  'derivative:\t{1!r}'.format(keys1, keys2))
        LOG.warn(u'Using common subset.')
        return list(OrderedSet(keys1) & OrderedSet(keys2))
    return keys1


def determine_ctd_keys(origin, deriv):
    """Return the coumns that should compose the key to merge on.

    The key is the pressure column, whether it is CTDPRS or REVPRS, or some
    other parameter.

    """
    keycol = None
    for param in PRESSURE_PARAMETERS:
        if param in origin and param in deriv:
            keycol = param
            break
    if keycol is None:
        raise ValueError(u'No available pressure columns to merge CTD on.')
    return (keycol,)


def merge_datafiles(origin, deriv, keys, parameters):
    """Merge the columns and data of two DataFiles."""
    row_map = map_keys(origin, deriv, keys)

    diffcols, not_in_orig_cols, not_in_deriv_cols, commoncols = \
        different_columns(origin, deriv, [], row_map)
    params_to_merge = filter_params_to_merge(
        diffcols, not_in_orig_cols, not_in_deriv_cols, commoncols, parameters)

    param_keys = set(params_to_merge) & set(keys)
    if param_keys:
        raise ValueError(
            u'Cannot merge key column using itself: {0!r}'.format(param_keys))

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

    all_cols = commoncols + not_in_deriv_cols + list(keys) + \
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
                        col.flags_woce, derivcol.flags_woce, row_map)
                if derivcol.flags_igoss:
                    col.flags_igoss = [9]
                    col.set_length(len(merged))
                    col.flags_igoss = overwrite_list(
                        col.flags_igoss, derivcol.flags_igoss, row_map)
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
                    col.values, derivcol.values, row_map)

    # Copy header from origin and add note about merged parameters
    header = '# Merged parameters: {0}\n# {1}\n'.format(
        ', '.join(params_to_merge), origin.globals['stamp'].rstrip())
    header_orig = origin.globals['header'].rstrip()
    if header_orig:
        header += header_orig + '\n'
    merged.globals['header'] = header

    return merged


def map_collections(origin, deriv, dfkeys=DFILE_KEY_COLS):
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


def merge_collections(origin, deriv, merge, dfkeys=DFILE_KEY_COLS):
    """Match up files in two archives and apply the merge function to them."""
    # Only merge files into the ones already present in origin. Warn if any
    # files from deriv are not used
    merged_dfc = DataFileCollection()
    dfile_map = map_collections(origin, deriv)
    for odfile, ddfile, dfkey in dfile_map:
        LOG.info(u'Merging files for key {0}'.format(dfkey))
        try:
            merged_dfc.append(merge(odfile, ddfile))
        except ValueError, err:
            LOG.error(
                u'Unable to merge datafiles for {0}: {1}'.format(dfkey, err))
    return merged_dfc
