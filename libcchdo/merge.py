from pandas import *

pandas_merge = merge

from numpy import isnan, object as np_object

from libcchdo import LOG
from libcchdo.formats import woce
from libcchdo.formats.exchange import END_DATA, FILL_VALUE, FLAG_WOCE_ENDING
from libcchdo.fns import equal_with_epsilon
from libcchdo.recipes.orderedset import OrderedSet
from libcchdo.model.datafile import DataFile


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
