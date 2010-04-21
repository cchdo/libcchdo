"""libcchdo.formats.ctd.oden"""

from sys import path
path.insert(0, '/'.join(path[0].split('/')[:-1]))

from libcchdo import Column
from format import format

class oden(format):

  def read(self, handle):
      """How to read a CTD ODEN file."""
      lineno = 1
      for line in handle:
          if lineno == 1:
              self.globals['SECT_ID'] = line[3:5] # Leg
              self.globals['STNNBR'] = line[7:10] # Station
              self.globals['CASTNO'] = line[13:15] # Cast
              self.globals['DATE'] = '19'+line[19:21]+line[17:19]+line[15:17]
              self.globals['TIME'] = line[41:45] # GMT Time(hhmm)
              #self.globals['cast_type'] = line[23:26]
              lat_deg = int(line[26:28])
              lat_min = float(line[28:32])
              lat_hem = line[32]
              if lat_hem == 'N':
                  self.globals['LATITUDE'] = str(lat_deg+lat_min/60)
              elif lat_hem == 'S':
                  self.globals['LATITUDE'] = str(-(lat_deg+lat_min/60))

              lng_deg = int(line[34:36])
              lng_min = float(line[36:40])
              lng_hem = line[40]
              if lng_hem == 'E':
                  self.globals['LONGITUDE'] = str(lng_deg+lng_min/60)
              elif lng_hem == 'W':
                  self.globals['LONGITUDE'] = str(-(lng_deg+lng_min/60))

              self.globals['DEPTH'] = line[45:50] # PDR Bottom Depth
              #self.globals['remarks'] = line[50:-1]

              self.columns['CTDPRS'] = Column('CTDPRS')
              self.columns['CTDTMP'] = Column('CTDTMP')
              self.columns['CTDCND'] = Column('CTDCND')
              self.columns['CTDSAL'] = Column('CTDSAL')
              self.columns['POTTMP'] = Column('POTTMP')
          else:
              data = line.split()
              row = lineno-2
              self.columns['CTDPRS'][row] = data[0]
              # need conversion from ITPS-68 to ITS-90?
              self.columns['CTDTMP'][row] = data[1]
              self.columns['CTDCND'][row] = data[2]
              self.columns['CTDSAL'][row] = data[3]
              self.columns['POTTMP'][row] = data[4]
          lineno += 1

  # OMIT writer
