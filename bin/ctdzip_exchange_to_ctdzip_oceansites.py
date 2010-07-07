#!/usr/bin/env python

from __future__ import with_statement

import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.ctd.zip.exchange as ctdzipex
import libcchdo.formats.ctd.zip.netcdf_oceansites as ctdzipnc_oceansites

if len(sys.argv) < 2:
    print 'Usage:', sys.argv[0], '<ctd exchange zip> [timeseries name]'
    exit(1)

file = libcchdo.DataFileCollection()
with open(sys.argv[1], 'r') as in_file:
    ctdzipex.read(file, in_file)

print >> sys.stderr, 'Done reading. Beginning CTD Zip write.'

if len(sys.argv) > 2:
    timeseries = sys.argv[2].strip()
    if timeseries == 'BATS':
        print >> sys.stderr, 'Printing a BATS OceanSITES NetCDF Zip'
        ctdzipnc_oceansites.write(file, sys.stdout, timeseries='BATS')
    elif timeseries == 'HOT':
        print >> sys.stderr, 'Printing a HOT OceanSITES NetCDF Zip'
        ctdzipnc_oceansites.write(file, sys.stdout, timeseries='HOT')
else:
    print >> sys.stderr, 'Printing an AMBIGUOUS (read: INVALID) OceanSITES NetCDF Zip'
    ctdzipnc_oceansites.write(file, sys.stdout)
