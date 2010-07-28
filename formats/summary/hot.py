""" libcchdo.formats.summary.hot """

import re
import datetime
import libcchdo.formats.woce

def read(self, handle):
    '''How to read a Summary file for HOT.'''
    header = True
    header_delimiter = re.compile('^-+$')
    for line in handle:
        if header:
            if header_delimiter.match(line):
                header = False
            else:
                self.header += line
        else:
          # TODO Reimplement by finding ASCII column edges in header and
          # reading that way. 
          # Spacing is unreliable.
          tokens = line.split()
          if len(tokens) is 0:
              continue
          self.columns['EXPOCODE'].append(tokens[0].replace('/', '_'))
          self.columns['SECT_ID'].append(tokens[1])
          self.columns['STNNBR'].append(int(tokens[2]))
          self.columns['CASTNO'].append(int(tokens[3]))
          self.columns['_CAST_TYPE'].append(tokens[4])
          date = datetime.datetime.strptime(tokens[5], '%m%d%y')
          self.columns['DATE'].append(
              "%4d%02d%02d" % (date.year, date.month, date.day))
          self.columns['TIME'].append(int(tokens[6]))
          self.columns['_CODE'].append(tokens[7])
          lat = libcchdo.formats.woce.woce_lat_to_dec_lat(tokens[8:11])
          self.columns['LATITUDE'].append(lat)
          lng = libcchdo.formats.woce.woce_lng_to_dec_lng(tokens[11:14])
          self.columns['LONGITUDE'].append(lng)
          self.columns['_NAV'].append(tokens[14])
          self.columns['DEPTH'].append(int(tokens[15]))
          self.columns['_ABOVE_BOTTOM'].append(int(tokens[16]))
          self.columns['_MAX_PRESSURE'].append(int(tokens[17]))
          self.columns['_NUM_BOTTLES'].append(int(tokens[18]))
          self.columns['_PARAMETERS'].append(tokens[19])
          self.columns['_COMMENTS'].append(' '.join(tokens[20:]))

    self.check_and_replace_parameters()

def write(self, handle):
    raise NotImplementedError # OMIT
