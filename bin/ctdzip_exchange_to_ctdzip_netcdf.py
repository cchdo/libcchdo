#!/usr/bin/env python

from __future__ import with_statement
import sys

import implib as L
import implib.model.datafile
import implib.formats.ctd.zip.exchange as ctdzipex
import implib.formats.ctd.zip.netcdf as ctdzipnc


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<ctd exchange zip>'
        return 1
    
    file = L.model.datafile.DataFileCollection()
    with open(argv[1], 'r') as in_file:
        ctdzipex.read(file, in_file)
    
    ctdzipnc.write(file, sys.stdout)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
