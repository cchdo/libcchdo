"""Utilities for operating on the CCHDO data directory"""

import re
import os
from shutil import copy2

from libcchdo import LOG


def intersection(self, o):
    return [x for x in self if x in o]


def all_in(a, b):
    return intersection(a, b) == a


def cd_to_data_directory():
  """Find the data directory.
  
  Find the data directory by checking for the existence of
  main_data_directories as subdirectories and sets it as the cwd.
  """
  directories_to_try = ['.', '/data', '/Volumes/DataArchive/data']
  main_data_directories = ['co2clivar', 'onetime', 'repeat']
  
  def is_root_data_dir():
      return all_in(main_data_directories, os.listdir('.'))

  found = False
  for dir in directories_to_try:
      LOG.info('Checking for data directory %s' % dir)
      os.chdir(dir)
      if is_root_data_dir():
          found = True
          break
  if not found:
      LOG.error('Unable to find data directory with subdirectories: %s' % \
                    ' '.join(main_data_directories))
      exit(1)
  LOG.info('Selected data directory %s' % os.getcwd())


datafile_extensions = ['su.txt', 'hy.txt', 'hy1.csv', 'ct.zip', 'ct1.zip',
                       'nc_hyd.zip', 'nc_ctd.zip']


def is_data_dir(dir):
    """Determine if the given path is a data directory.

    """
    def filename_has_any_extensions(filename, extensions):
        return any(map(lambda e: filename.endswith(e), extensions))

    return any(map(lambda f: filename_has_any_extensions(
                   f, datafile_extensions),
               os.listdir(dir)))


def is_cruise_dir(dir):
    """Determine if the given path is a cruise directory.

    Basically, if the 'ExpoCode' file exists, it's a cruise directory.

    """
    return 'ExpoCode' in os.listdir(dir)


allowable_oceans = ['arctic', 'atlantic', 'pacific', 'indian', 'southern']


_blacklisted_dirnames = [
    'WORK', 'work', 'TMP', 'tmp',
    'original', 'ORIG', 'ORIGINAL',
    'Queue',
    '.svn',
    'CTD', 'BOT',
    'old', 'prnt_dir'
]


_blacklisted_dirname_regexps = ['temp', 'tmp', 'holder', 'removed', 'moved']


def do_for_cruise_directories(operation):
  """Call operation for every cruise directory.

  Traverses the cwd as the CCHDO data directory and calls
  operation(fullpath_to_dir, dirs, files) for each cruise directory.
  """
  cd_to_data_directory()

  # Traverse the data directory to find real data directories to operate on
  main_data_directories = ['co2clivar', 'onetime', 'repeat']
  for dir in main_data_directories:
      for root, dirs, files in os.walk(dir, topdown=True):
          # Filter out unwanted directories if still traveling down tree
          for dir in dirs[:]:
              if dir in _blacklisted_dirnames:
                  dirs.remove(dir)
              else:
                  for black in _blacklisted_dirname_regexps:
                      if re.search(black, dir, re.IGNORECASE):
                          dirs.remove(dir)
                          break
          # Only operate if this is a data directory
          if is_data_dir(root):
              operation(root, dirs, files)


def mkdir_ensure(path, mode=0777):
    try:
        os.mkdir(path, mode)
        os.chmod(path, mode)
    except OSError:
        makedirs_ensure(path, mode)


def makedirs_ensure(path, mode=0777):
    try:
        os.makedirs(path, mode)
        os.chmod(path, mode)
    except OSError:
        pass


def copy(src, dst, mode=0664):
    copy2(src, dst)

    dst_path = dst
    if os.path.isdir(dst):
        dst_path = os.path.join(dst, os.path.basename(src))
    os.chmod(dst_path, mode)


def _make_subdir(root, dirname, perms):
    subroot = os.path.join(root, dirname)
    mkdir_ensure(subroot, perms)
    os.chmod(subroot, perms)


def make_subdirs(root, subdirs, perms):
    for subdir in subdirs:
        if type(subdir) is list:
            subdir, subdirs = subdir[0], subdir[1]
            _make_subdir(root, subdir, perms)
            make_subdirs(os.path.join(root, subdir), subdirs, perms)
        else:
            _make_subdir(root, subdir, perms)
