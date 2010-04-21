#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, path, stdout, stderr
path.insert(0, '/'.join(path[0].split('/')[:-1]))

from libcchdo import DataFile
from formats.ctd.netcdf_oceansites import netcdf_oceansites

if len(argv) < 3:
    print 'Usage:', argv[0], '<cchdo_netcdf> [timeseries name]'
    exit(1)

file = libcchdo.DataFile()
with open(argv[1], 'r') as in_file:
    file.read_CTD_NetCDF(in_file)

if argv[2]:
    timeseries = argv[2].strip()
    if timeseries == 'BATS':
        print >> stderr, 'Printing a BATS OceanSITES NetCDF'
        netcdf_oceansites(file).write(stdout, timeseries='BATS')
    elif timeseries == 'HOT':
        print >> stderr, 'Printing a HOT OceanSITES NetCDF'
        netcdf_oceansites(file).write(stdout, timeseries='HOT')
else:
    print >> stderr, 'Printing an AMBIGUOUS (read: INVALID) OceanSITES NetCDF'
    file.write_CTD_NetCDF_OceanSITES(out_file)
