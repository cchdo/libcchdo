#!/usr/bin/env python

from __future__ import with_statement
import sys

import abs_import_libcchdo
import libcchdo.model.datafile
import libcchdo.formats.ctd.zip.exchange as ctdzipex
import libcchdo.formats.ctd.zip.netcdf_oceansites as ctdzipnc_oceansites


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<ctd exchange zip> [timeseries name]'
        return 1
    
    file = libcchdo.model.datafile.DataFileCollection()
    with open(argv[1], 'r') as in_file:
        ctdzipex.read(file, in_file)
    
    print >> sys.stderr, 'Done reading. Beginning CTD Zip write.'
    
    if len(argv) > 2:
        timeseries = argv[2].strip()
        if timeseries == 'BATS':
            print >> sys.stderr, 'Printing a BATS OceanSITES NetCDF Zip'
            ctdzipnc_oceansites.write(file, sys.stdout, timeseries='BATS')
        elif timeseries == 'HOT':
            print >> sys.stderr, 'Printing a HOT OceanSITES NetCDF Zip'
            ctdzipnc_oceansites.write(file, sys.stdout, timeseries='HOT')
    else:
        print >> sys.stderr, 'Printing an AMBIGUOUS (read: INVALID) OceanSITES NetCDF Zip'
        ctdzipnc_oceansites.write(file, sys.stdout)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
