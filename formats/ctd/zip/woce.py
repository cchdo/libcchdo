''' libcchdo.ctd.zip.woce '''

from tempfile import mkdtemp
from StringIO import StringIO
from zipfile import ZipFile, ZipInfo

from ..woce import woce
from ..format import format

class woce(format):
  def read(self, handle):
    '''How to read CTD WOCE files from a Zip.'''
    zip = ZipFile(handle, 'r')
    for file in zip.namelist():
      if 'README' in file or 'DOC' in file: continue
      tempstream = StringIO(zip.read(file))
      ctdfile = DataFile()
      woce(ctdfile).read(tempstream)
      self.datafile.files.append(ctdfile)
      tempstream.close()
    zip.close()
  #def write(self, handle): TODO
  #  '''How to write CTD WOCE files to a Zip.'''
