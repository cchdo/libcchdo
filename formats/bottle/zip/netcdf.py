''' libcchdo.bottle.zip.netcdf '''

from tempfile import mkdtemp
from StringIO import StringIO
from zipfile import ZipFile, ZipInfo

from netCDF3 import DataSet

from ..netcdf import netcdf
from ..format import format

class netcdf(format):
  def read(self, handle):
    '''How to read Bottle NetCDF files from a Zip.'''
    zip = ZipFile(handle, 'r')
    for file in zip.namelist():
      if '.csv' not in file: continue
      tempstream = StringIO(zip.read(file))
      bottlefile = DataFile()
      netcdf(bottlefile).read(tempstream)
      self.datafile.files.append(bottlefile)
      tempstream.close()
    zip.close()
  def write(self, handle):
    '''How to write Bottle NetCDF files to a Zip.
    The collection should already be split apart based on station cast.
    '''
    # NetCDF libraries like to write to a file. Work around by giving temp dir.
    tempdir = mkdtemp()

    for file in self.datafile.files:
      netcdf(file).write(tempdir)

    zip = ZipFile(handle, 'w')
    for file in listdir(tempdir):
      fullpath = tempdir+'/'+file
      zip.write(fullpath)
      remove(fullpath)
    rmdir(tempdir)
    zip.close()
