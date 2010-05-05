#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, path, stdout, stderr
path.insert(0, '/'.join(path[0].split('/')[:-1]))

import libcchdo
import formats.ctd.netcdf as ctdnc
import formats.ctd.netcdf_oceansites as ctdnc_oceansites

if len(argv) < 3:
    print 'Usage:', argv[0], '<cchdo_netcdf> [timeseries name]'
    exit(1)

file = libcchdo.DataFile()
with open(argv[1], 'r') as in_file:
    ctdnc.read(file, in_file)

if argv[2]:
    timeseries = argv[2].strip()
    if timeseries == 'BATS':
        print >> stderr, 'Printing a BATS OceanSITES NetCDF'
        ctdnc_oceansites.write(file, stdout, timeseries='BATS')
    elif timeseries == 'HOT':
        print >> stderr, 'Printing a HOT OceanSITES NetCDF'
        ctdnc_oceansites.write(file, stdout, timeseries='HOT')
else:
    print >> stderr, 'Printing an AMBIGUOUS (read: INVALID) OceanSITES NetCDF'
    ctdnc_oceansites.write(file, stdout)
