#!/usr/bin/env python

from __future__ import with_statement

import sys

import implib as L
import implib.model.datafile
import implib.formats.ctd.netcdf as ctdnc
import implib.formats.ctd.netcdf_oceansites as ctdnc_oceansites


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<cchdo_netcdf> [timeseries name]'
        return 1
    
    file = L.model.datafile.DataFile()
    with open(argv[1], 'r') as in_file:
        ctdnc.read(file, in_file)
    
    if len(argv) > 2:
        timeseries = argv[2].strip()
        if timeseries == 'BATS':
            print >> sys.stderr, 'Printing a BATS OceanSITES NetCDF'
            ctdnc_oceansites.write(file, sys.stdout, timeseries='BATS')
        elif timeseries == 'HOT':
            print >> sys.stderr, 'Printing a HOT OceanSITES NetCDF'
            ctdnc_oceansites.write(file, sys.stdout, timeseries='HOT')
    else:
        print >> sys.stderr, 'Printing an AMBIGUOUS (read: INVALID) OceanSITES NetCDF'
        ctdnc_oceansites.write(file, sys.stdout)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
