''' libcchdo.ctd.zip.exchange '''

from StringIO import StringIO
from zipfile import ZipFile, ZipInfo

from ..oden import oden
from ..format import format

class exchange(format):
  def read(self, handle):
    '''How to read CTD ODEN files from a Zip.'''
    zip = ZipFile(handle, 'r')
    for file in zip.namelist():
      if 'DOC' in file or 'README' in file:
        continue
      tempstream = StringIO(zip.read(file))
      ctdfile = DataFile()
      oden(ctdfile).read(tempstream)
      self.datafile.files.append(ctdfile)
      tempstream.close()
    zip.close()
  # OMIT writer
