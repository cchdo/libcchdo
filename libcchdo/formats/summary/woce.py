import re
import datetime

from .. import woce


def read(self, handle):
    '''How to read a Summary file for WOCE.'''
    header = True
    header_delimiter = re.compile('^-+$')
    column_starts = []
    column_widths = []
    for line in handle:
        if header:
            if header_delimiter.match(line):
                header = False
                # Stops are tuples (beginning of column, end of column)
                # This is to delimit the columns of the sumfile
                stops = re.finditer('(\w+\s*)', self.globals['header'].split('\n')[-2])
                for stop in stops:
                    start = stop.start()
                    if len(column_starts) is 0:
                        column_starts.append(0)
                    else:
                        column_starts.append(start)
                    column_widths.append(stop.end()-start)
            else:
                self.globals['header'] += line
        else:
            tokens = []
            for s, w in zip(column_starts, column_widths):
                tokens.append(line[:-1][s:s+w].strip())
            def identity_or_none(x):
                return x if x else None
            def int_or_none(x):
                return int(x) if x and x.isdigit() else None
            if len(tokens) is 0: continue
            cs = self.columns
            cs['EXPOCODE'].append(tokens[0].replace('/', '_'))
            cs['SECT_ID'].append(tokens[1])
            cs['STNNBR'].append(int_or_none(tokens[2]))
            cs['CASTNO'].append(int_or_none(tokens[3]))
            cs['_CAST_TYPE'].append(tokens[4])
            date = datetime.datetime.strptime(tokens[5], '%m%d%y')
            cs['DATE'].append('%4d%02d%02d' % \
                              (date.year, date.month, date.day))
            cs['TIME'].append(int_or_none(tokens[6]))
            cs['_CODE'].append(tokens[7])
            lat = woce.woce_lat_to_dec_lat(tokens[8].split())
            cs['LATITUDE'].append(lat)
            lng = woce.woce_lng_to_dec_lng(tokens[9].split())
            cs['LONGITUDE'].append(lng)
            cs['_NAV'].append(tokens[10])
            cs['DEPTH'].append(int_or_none(tokens[11]))
            cs['_ABOVE_BOTTOM'].append(int_or_none(tokens[12]))
            cs['_WIRE_OUT'].append(int_or_none(tokens[13]))
            cs['_MAX_PRESSURE'].append(int_or_none(tokens[14]))
            cs['_NUM_BOTTLES'].append(int_or_none(tokens[15]))
            cs['_PARAMETERS'].append(identity_or_none(tokens[16]))
            cs['_COMMENTS'].append(identity_or_none(tokens[17]))

    self.check_and_replace_parameters()


def write(self, handle):
    '''How to write a Summary file for WOCE.'''
    today = datetime.date.today()
    uniq_sects = uniquify(self.columns['SECT_ID'].values)
    handle.write('R/V _SHIP LEG _# WHP-ID '+','.join(uniq_sects)+
                 ' %04d%02d%02d' % (today.year, today.month, today.day)+
                 "SIOCCHDOLIB\n")
    header_one = 'SHIP/CRS       WOCE               CAST         UTC           POSITION                UNC   COR ABOVE  WIRE   MAX  NO. OF\n'
    header_two = 'EXPOCODE       SECT STNNBR CASTNO TYPE DATE   TIME CODE LATITUDE   LONGITUDE   NAV DEPTH DEPTH BOTTOM  OUT PRESS BOTTLES PARAMETERS      COMMENTS            \n'
    header_sep = ('-' * (len(header_two)-1)) + '\n'
    handle.write(header_one)
    handle.write(header_two)
    handle.write(header_sep)
    for i in range(0, len(self)):
        exdate = self.columns['DATE'][i]
        date_str = exdate[4:6]+exdate[6:8]+exdate[2:4]
        row = ('%-14s %-5s %5s    %3d  %3s %-6s %04d   %2s %-10s %-11s %3s %5d       %-6d      %5d %7d %-15s %-20s' %
          (self.columns['EXPOCODE'][i], self.columns['SECT_ID'][i],
           self.columns['STNNBR'][i], self.columns['CASTNO'][i],
           self.columns['_CAST_TYPE'][i], date_str,
           self.columns['TIME'][i], self.columns['_CODE'][i],
           formats.woce.dec_lat_to_woce_lat(self.columns['LATITUDE'][i]),
           formats.woce.dec_lng_to_woce_lng(self.columns['LONGITUDE'][i]),
           self.columns['_NAV'][i], self.columns['DEPTH'][i],
           self.columns['_ABOVE_BOTTOM'][i],
           self.columns['_MAX_PRESSURE'][i],
           self.columns['_NUM_BOTTLES'][i], self.columns['_PARAMETERS'][i],
           self.columns['_COMMENTS'][i]))
        handle.write(row+'\n')
    handle.close()
