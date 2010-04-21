"""libcchdo.ctd.zip.exchange"""

from tempfile import mkdtemp
from StringIO import StringIO
from zipfile import ZipFile, ZipInfo
from datetime import datetime

from libcchdo import DataFile
import ctd.exchange
from ..format import format

class exchange(format):

  def read(self, handle):
    """How to read CTD Exchange files from a Zip."""
    zip = ZipFile(handle, 'r')
    for file in zip.namelist():
        if '.csv' not in file: continue
        tempstream = StringIO(zip.read(file))
        ctdfile = DataFile()
        ctd.exchange.exchange(ctdfile).read(tempstream)
        self.datafile.files.append(ctdfile)
        tempstream.close()
    zip.close()

  def write(self, handle):
      """How to write CTD Exchange files to a Zip."""
      zip = ZipFile(handle, 'w')
      for file in self.datafile.files:
          tempstream = StringIO()
          ctd.exchange.exchange(file).write(tempstream)
          station = int(file.globals['STNNBR'].strip())
          cast = int(file.globals['CASTNO'].strip())
          info = ZipInfo('%s_%05d_%05d_ct1.csv' % \
                         (file.globals['EXPOCODE'], station, cast))
          dt = datetime.now()
          info.date_time = (dt.year, dt.month, dt.day,
                            dt.hour, dt.minute, dt.second)
          info.external_attr = 0644 << 16L
          zip.writestr(info, tempstream.getvalue())
          tempstream.close()
      zip.close()
