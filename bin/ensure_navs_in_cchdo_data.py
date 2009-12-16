#!/usr/bin/env python

from __future__ import with_statement
from logging import debug, info, warning, error
from os import path
from sys import argv, exit
import struct

import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-1]))
import libcchdo
from datadir.util import do_for_cruise_directories

def ensure_nav(root, dirs, files):
  navfiles = filter(lambda f: f.endswith('na.txt'), files)
  if len(navfiles) > 0:
    info('%s has nav files %s' % (root, ', '.join(navfiles)))
  else:
    info('%s is missing a nav file. Attempting to generate one.' % root)
    # Try to use easiest generation method first
    generation_methods = [
                          ['Bottle Exchange', 'hy1.csv', libcchdo.DataFile.read_Bottle_Exchange],
                          ['Summary', 'su.txt', libcchdo.SummaryFile.read_Summary_WOCE],
                          # Other WOCE files do not have lng lat (they're in the Summary file)
                          # TODO Collections have to have some regular way to be merged before they can be outputted to nav.
                          #['CTD Exchange', 'ct1.zip', libcchdo.DataFileCollection.read_CTDZip_Exchange],
                          #['Bottle NetCDF', 'nc_hyd.zip', libcchdo.DataFileCollection.read_BottleZip_NetCDF],
                          #['CTD NetCDF', 'nc_ctd.zip', libcchdo.DataFileCollection.read_CTDZip_NetCDF],
                         ]
    for methodname, extension, readfn in generation_methods:
      basefiles = filter(lambda f: f.endswith(extension), files)
      if len(basefiles) > 0:
        info('  Found a %s file.' % methodname)
        for file in basefiles:
          try:
            outputfile = '%sna.txt' % file[:-len(extension)]
            info('  Generating nav file %s from a %s file %s.' % (outputfile, methodname, file))
            fh = readfn.im_class()
            with open(path.join(root, file), 'r') as in_file:
              readfn(fh, in_file)
            #with open(path.join(root, outputfile), 'w') as out_file:
            #  fh.write_nav(out_file)
            from sys import stdout
            print fh
            fh.write_nav(stdout)
            return True
          except NotImplementedError, e:
            info('Unable to generate. The read function has not been implemented: %s' % e)
          except struct.error, e1:
            info('  Ignoring WOCE unpack error and continuing with different method: %s' % e1)
          except NameError, e2:
            if str(e2).endswith("not in CCHDO's parameter list."):
              info('  Ignoring parameter not in database error.')
            else:
              warning('  Ignoring exception: %s' % e2)
          except ValueError, e3:
            if str(e3).startswith("time data did not match format"):
              info('  Ignoring time data format error: %s' % e3)
            else:
              warning('  Ignoring exception: %s' % e3)
          except Exception, ee:
            warning('  Ignoring exception: %s' % ee)
      info('  Unable to find a %s file.' % methodname)
    warning('  Unable to generate a nav file for %s' % root)
    return False

do_for_cruise_directories(ensure_nav)
