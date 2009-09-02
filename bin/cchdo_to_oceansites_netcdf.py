#!/usr/bin/env python

from __future__ import with_statement
import libcchdo

from sys import argv
if len(argv) < 3:
  print 'Usage:', argv[0], '<cchdo_netcdf> <oceansites_destination>'
  exit(1)
file = libcchdo.DataFile()
with open(argv[1], 'r') as in_file:
  file.read_CTD_NetCDF(in_file)
with open(argv[2], 'w') as out_file:
  file.write_CTD_NetCDF_OceanSITES(out_file)

libcchdo.disconnect()
