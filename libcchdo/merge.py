from pandas import *

from libcchdo import LOG
from libcchdo.formats import woce
from libcchdo.fns import equal_with_epsilon


class Merger(object):
    GROUP_COLS = ['STNNBR', 'CASTNO', 'SAMPNO']
    DONT_MERGE = [
        "Fake", "STNNBR", "CASTNO", "BTLNBR", "BTLNBR_FLAG_W", "DATE", "DEPTH",
        "EXPOCODE", "CTDPRS", "CTDTMP", "SECT_ID", "LATITUDE", "LONGITUDE",
        "CTDSAL", "CTDSAL_FLAG_W", "SAMPNO", "TIME"]

    def __init__(self, file1, file2):
        self.datafile1 = file1
        self.datafile2 = file2
        self.header_cnt1, self.header1, self.units1, self.stamp1 = self.count_headers(self.datafile1)
        self.header_cnt2, self.header2, self.units2, self.stamp2 = self.count_headers(self.datafile2)
        skipped_lines1 = [self.header_cnt1 + 1]
        skipped_lines2 = [self.header_cnt2 + 1]
        if self.check_last_line(self.datafile1):
            self.dataframe1 = read_csv(self.datafile1, header=(self.header_cnt1 ), skiprows=skipped_lines1, skip_footer=1)
            self.dataframe2 = read_csv(self.datafile2, header=(self.header_cnt2 ), skiprows=skipped_lines2, skip_footer=1)
        else:
            self.dataframe1 = read_csv(self.datafile1, header=(self.header_cnt1 ), skiprows=skipped_lines1)
            self.dataframe2 = read_csv(self.datafile2, header=(self.header_cnt2 ), skiprows=skipped_lines2)

    def count_headers(self, file_handle):
        header_cnt = 1 
        header = ""
        stamp = file_handle.readline()
        if not stamp.startswith('BOTTLE'):
            raise ValueError('Stamp {0!r} must start with BOTTLE'.format(stamp))
        line = file_handle.readline()
        while line:
            if line.startswith('#'):
                header_cnt += 1
                header += line
            else:
                break
            line = file_handle.readline()
        units_line = file_handle.readline().rstrip()
        units = units_line.split(',')
        file_handle.seek(0,0)
        return header_cnt, header, units, stamp

    def check_last_line(self, file_handle):
        lines = file_handle.readlines()
        file_handle.seek(0,0)
        if lines[-1].startswith(woce.END_DATA):
            return True 
        else:
            return False

    def merge_cols(self):
        return self.dataframe2.columns - DONT_MERGE

    def different_cols(self):
        different_cols = []
        df1_grouped = self.dataframe1.groupby(self.GROUP_COLS, axis=0)
        df2_grouped = self.dataframe2.groupby(self.GROUP_COLS, axis=0)

        row_map = []
        for cast_identifier, group in df2_grouped:
            if cast_identifier not in df1_grouped.groups.keys():
                continue
            # Find the rows that correspond to the same data row
            row1 = df1_grouped.groups[cast_identifier][0]
            row2 = df2_grouped.groups[cast_identifier][0]
            row_map.append([row1, row2])

        for col in self.dataframe2:
            if col not in self.dataframe1.columns:
                if col not in different_cols:
                    different_cols.append(col)
                continue
            LOG.debug('checking {0}'.format(col))

            for row1, row2 in row_map:
                # Make sure the values for both dataframes matches
                df1col = self.dataframe1[col]
                df2col = self.dataframe2[col]
                val1 = df1col[row1]
                val2 = df2col[row2]
                if val1 != val2 and not equal_with_epsilon(val1, val2):
                    LOG.info(u'{0} differs at {1}:\t{2!r} {3!r}'.format(col,
                        cast_identifier, val1, val2))
                    if col not in different_cols:
                        different_cols.append(col)
                    # TODO why set equal?
                    df1col[row1] = df2col[row2]
        return different_cols
        
    def mergeit(self,columns_to_merge):
        df1_grouped = self.dataframe1.groupby(self.GROUP_COLS,axis=0)
        df2_grouped = self.dataframe2.groupby(self.GROUP_COLS,axis=0)

        for col in columns_to_merge:
            if col not in self.dataframe1.columns:
                temp_frame = []
                temp_frame = self.dataframe2.copy(deep=True)
                for col_check in self.dataframe2.columns:
                    if col_check not in self.GROUP_COLS + [col]:
                        del temp_frame[col_check]
                self.dataframe1 = merge(self.dataframe1, temp_frame ,how='outer', on=self.GROUP_COLS)
                self.dataframe1 = self.dataframe1.fillna(-999.00)
       
        return self.dataframe1


def convert_to_datafile(self, header, dataframe, units, stamp):
    self.globals['header'] = header
    self.globals['stamp'] = stamp
    columns = dataframe.columns
    self.create_columns(columns, units)
    for param in columns:
        try:
            param.index('FLAG')
            continue
        except ValueError:
            pass
        self[param].values = dataframe[param]
        if (param + "_FLAG_W") in columns:
            self[param].flags_woce = dataframe[param + "_FLAG_W"]

    self.check_and_replace_parameters()
    woce.fuse_datetime(self)
    return self
