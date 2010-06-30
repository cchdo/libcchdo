#!/usr/bin/env python

from __future__ import with_statement

import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.ctd.netcdf as ctdnc
import libcchdo.formats.ctd.netcdf_oceansites as ctdnc_oceansites

if len(sys.argv) < 2:
    print 'Usage:', sys.argv[0], '<cchdo_netcdf> [timeseries name]'
    exit(1)

file = libcchdo.DataFile()
with open(sys.argv[1], 'r') as in_file:
    ctdnc.read(file, in_file)

if len(sys.argv) >= 2:
    timeseries = sys.argv[2].strip()
    if timeseries == 'BATS':
        print >> sys.stderr, 'Printing a BATS OceanSITES NetCDF'
        ctdnc_oceansites.write(file, sys.stdout, timeseries='BATS')
    elif timeseries == 'HOT':
        print >> sys.stderr, 'Printing a HOT OceanSITES NetCDF'
        ctdnc_oceansites.write(file, sys.stdout, timeseries='HOT')
else:
    print >> sys.stderr, 'Printing an AMBIGUOUS (read: INVALID) OceanSITES NetCDF'
    ctdnc_oceansites.write(file, sys.stdout)
