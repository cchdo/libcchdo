# libcchdo.google_wire.format

from sys import argv, exit, path
path.insert(0, '/'.join(path[0].split('/')[:-1]))
import libcchdo

class format:
  def __init__(self, datafile):
    self.datafile = datafile
  def read(self, handle):
    raise NotImplementedError
  def write(self, handle):
    raise NotImplementedError
