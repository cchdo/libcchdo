'''Utilities for operating on the CCHDO data directory'''

from logging import getLogger, DEBUG, StreamHandler, debug, info, warning, error
from re import search, IGNORECASE
from os import chdir, listdir, path, walk, getcwd

from sys import argv, exit, path
path.insert(0, '/'.join(path[0].split('/')[:-1]))
import libcchdo

logger = getLogger()
logger.setLevel(DEBUG)
logger.addHandler(StreamHandler())

def intersection(self, o):
  return [x for x in self if x in o]
def all_in(a, b):
  return intersection(a, b) == a

def cd_to_data_directory():
  '''
  Find the data directory by checking for the existence of
  main_data_directories as subdirectories and sets it as the cwd.
  '''
  directories_to_try = ['.', '/data', '/Volumes/DataArchive/data']
  main_data_directories = ['co2clivar', 'onetime', 'repeat']
  
  def is_root_data_dir():
    return all_in(main_data_directories, listdir('.'))
  found = False
  for dir in directories_to_try:
    info('Checking for data directory %s' % dir)
    chdir(dir)
    if is_root_data_dir():
      found = True
      break
  if not found:
    error('Unable to find data directory with subdirectories: %s' % ' '.join(main_data_directories))
    exit(1)
  info('Selected data directory %s' % getcwd())

datafile_extensions = ['su.txt', 'hy.txt', 'hy1.csv', 'ct.zip', 'ct1.zip',
                       'nc_hyd.zip', 'nc_ctd.zip']
def is_data_dir(dir):
  def filename_has_any_extensions(filename, extensions):
    return any(map(lambda e: filename.endswith(e), extensions))
  return any(map(lambda f: filename_has_any_extensions(f, datafile_extensions),
                 listdir(dir)))
allowable_oceans = ['arctic', 'atlantic', 'pacific', 'indian', 'southern']

def do_for_cruise_directories(operation):
  '''
  Traverses the cwd as the CCHDO data directory and calls
  operation(fullpath_to_dir, dirs, files) for each cruise directory.
  '''
  cd_to_data_directory()
  blacklisted_dirnames = ['WORK', 'work', 'TMP', 'tmp',
                          'original', 'ORIG', 'ORIGINAL',
                          'Queue',
                          '.svn',
                          'CTD', 'BOT',
                          'old', 'prnt_dir']
  blacklisted_dirname_regexps = ['temp', 'tmp', 'holder', 'removed', 'moved']
  # Traverse the data directory to find real data directories to operate on
  main_data_directories = ['co2clivar', 'onetime', 'repeat']
  for dir in main_data_directories:
    for root, dirs, files in walk(dir, topdown=True):
      # Filter out unwanted directories if still traveling down tree
      for dir in dirs[:]:
        if dir in blacklisted_dirnames:
          dirs.remove(dir)
        else:
          for black in blacklisted_dirname_regexps:
            if search(black, dir, IGNORECASE):
              dirs.remove(dir)
              break
      # Only operate if this is a data directory
      if is_data_dir(root):
        operation(root, dirs, files)
