from pandas import *


class Merger(object):
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
            raise Exception
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
        df1_grouped = self.dataframe1.groupby(['STNNBR', 'CASTNO', 'SAMPNO'],axis=0);
        df2_grouped = self.dataframe2.groupby(['STNNBR', 'CASTNO', 'SAMPNO'],axis=0);
        cols1, rows1 = self.dataframe1.shape 
        cols2, rows2 = self.dataframe2.shape 

        for key, group in df2_grouped:
            if key in df1_grouped.groups.keys():
                row1 = df1_grouped.groups[key]
                row2 = df2_grouped.groups[key]
                for col in self.dataframe2:
                    if col in self.dataframe1.columns:
                        x = self.dataframe1[col]
                        y = self.dataframe2[col]
                        if x[row1[0]] != y[row2[0]]:
                            if col not in different_cols:
                                different_cols.append(col)
                            self.dataframe1[col][row1[0]] = self.dataframe2[col][row2[0]]
                    elif col not in different_cols:
                        different_cols.append(col)
        return different_cols
        
    def mergeit(self,columns_to_merge):
        df1_grouped = self.dataframe1.groupby(['STNNBR', 'CASTNO', 'SAMPNO'],axis=0);
        df2_grouped = self.dataframe2.groupby(['STNNBR', 'CASTNO', 'SAMPNO'],axis=0);
        cols1, rows1 = self.dataframe1.shape 
        cols2, rows2 = self.dataframe2.shape 

        for col in columns_to_merge:
            if col not in self.dataframe1.columns:
                temp_frame = []
                temp_frame = self.dataframe2.copy(deep=True)
                for col_check in self.dataframe2.columns:
                    if col_check not in ['STNNBR', 'CASTNO', 'SAMPNO', col]:
                        del temp_frame[col_check]
                self.dataframe1 = merge(self.dataframe1, temp_frame ,how='outer', on=['STNNBR','CASTNO','SAMPNO'])
                self.dataframe1 = self.dataframe1.fillna(-999.00)
       
        return self.dataframe1


def convert_to_datafile(self, header, dataframe, units, stamp):
    self.globals['header'] = header
    self.globals['stamp'] = stamp.rstrip()
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
