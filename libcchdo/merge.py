from pandas import *

from numpy import isnan

from libcchdo import LOG
from libcchdo.formats import woce
from libcchdo.formats.exchange import END_DATA, FILL_VALUE, FLAG_WOCE_ENDING
from libcchdo.fns import equal_with_epsilon
from libcchdo.recipes.orderedset import OrderedSet
from libcchdo.model.datafile import DataFile


KEY_COLS = ['STNNBR', 'CASTNO', 'SAMPNO']


class MergeData(object):
    def __init__(self, stamp, header, param_units, dframe):
        self.stamp = stamp
        self.header = header
        self.param_units = param_units
        self.dframe = dframe

    def grouped(self):
        return self.dframe.groupby(KEY_COLS, axis=0)

    def convert_to_datafile(self, parameters):
        dfile = DataFile()

        header = '# Merged parameters: {0}\n#{1}\n{2}\n'.format(
            ', '.join(parameters), self.stamp, '\n'.join(self.header))
        dfile.globals['header'] = header

        params = self.dframe.head(1)
        units = [self.param_units[param] for param in params]
        dfile.create_columns(params, units)

        for param in params:
            try:
                param.index('FLAG')
                continue
            except ValueError:
                pass
            dfile[param].values = self.dframe[param]
            flag_name = param + FLAG_WOCE_ENDING
            if flag_name in params:
                dfile[param].flags_woce = self.dframe[flag_name]

        dfile.check_and_replace_parameters()
        woce.fuse_datetime(dfile)
        return dfile


class Merger(object):
    def __init__(self, file1, file2):
        self.mdata1 = self.read_file(file1)
        self.mdata2 = self.read_file(file2)

    def read_file(self, fobj):
        # TODO this should really be a read into libcchdo.model.datafile format
        # and then converted into a pandas dataframe.
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

    def different_cols(self):
        df1_grouped = self.mdata1.grouped()
        df2_grouped = self.mdata2.grouped()

        row_map = []
        for cast_identifier, group in df2_grouped:
            if cast_identifier not in df1_grouped.groups.keys():
                continue
            # Find the rows that correspond to the same data row
            row1 = df1_grouped.groups[cast_identifier][0]
            row2 = df2_grouped.groups[cast_identifier][0]
            row_map.append([row1, row2])

        different_cols = OrderedSet()
        dframe2 = self.mdata2.dframe
        dframe1 = self.mdata1.dframe
        columns2 = dframe2.columns
        columns1 = dframe1.columns
        for col in columns2:
            if col not in columns1:
                different_cols.add(col)
                continue

            for row1, row2 in row_map:
                # Make sure the values for both dataframes matches
                val1 = dframe1[col][row1]
                val2 = dframe2[col][row2]
                if val1 != val2 and not equal_with_epsilon(val1, val2):
                    LOG.info(u'{0} differs at {1}:\t{2!r} {3!r}'.format(col,
                        cast_identifier, val1, val2))
                    different_cols.add(col)
        return list(different_cols)

    def _available_keys(self):
        """Return keys that are available to merge on."""
        df1_cols = list(self.mdata1.dframe.columns)
        return [col for col in KEY_COLS if col in df1_cols]
        
    def merge(self, columns_to_merge):
        df1_cols = list(self.mdata1.dframe.columns)
        on_cols = self._available_keys()

        LOG.debug('Merging using keys: {0!r}'.format(on_cols))

        for col in columns_to_merge:
            LOG.debug('merging {0!r}'.format(col))
            # Adding new column
            if col not in df1_cols:
                temp_frame = self.mdata2.dframe.copy(deep=True)
                for col_to_check in self.mdata2.dframe.columns:
                    if col_to_check not in KEY_COLS + [col]:
                        del temp_frame[col_to_check]
                self.mdata1.dframe = merge(
                    self.mdata1.dframe, temp_frame, how='outer', on=on_cols)
                # If the new file has less records, fill in the missing records
                fill_map = {}
                for param in self.mdata1.dframe.head(1):
                    if param.endswith(FLAG_WOCE_ENDING):
                        fill_map[param] = 9
                    else:
                        fill_map[param] = FILL_VALUE
                self.mdata1.dframe = self.mdata1.dframe.fillna(fill_map)
            # Updating column data
            else:
                temp_frame = self.mdata2.dframe.copy(deep=True)
                for col_to_check in self.mdata2.dframe.columns:
                    if col_to_check not in KEY_COLS + [col]:
                        del temp_frame[col_to_check]
                self.mdata1.dframe.update(temp_frame)

        param_units = dict(self.mdata1.param_units)
        param_units.update(self.mdata2.param_units)
        return MergeData(self.mdata1.stamp, self.mdata1.header, param_units,
                         self.mdata1.dframe)
