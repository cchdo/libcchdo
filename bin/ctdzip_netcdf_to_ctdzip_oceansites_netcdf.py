#!/usr/bin/env python

import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-1]))
from __future__ import with_statement
import libcchdo

from sys import argv
if len(argv) < 3:
  print 'Usage:', argv[0], '<cchdo_netcdf> <oceansites_destination> [timeseries name]'
  exit(1)
file = libcchdo.DataFile()
with open(argv[1], 'r') as in_file:
  file.read_CTD_NetCDF(in_file)
with open(argv[2], 'w') as out_file:
  argv[3] = argv[3].strip()
  if argv[3] == 'BATS':
    print 'Printing a BATS OceanSITES NetCDF'
    file.write_CTD_NetCDF_OceanSITES_BATS(out_file)
  elif argv[3] == 'HOT':
    print 'Printing a HOT OceanSITES NetCDF'
    file.write_CTD_NetCDF_OceanSITES_HOT(out_file)
  else:
    print 'Printing an ambiguous OceanSITES NetCDF'
    file.write_CTD_NetCDF_OceanSITES(out_file)

