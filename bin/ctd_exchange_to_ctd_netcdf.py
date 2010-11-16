#!/usr/bin/env python

from __future__ import with_statement
import sys

import implib
import libcchdo.model.datafile
import libcchdo.formats.ctd.netcdf as ctdnc
import libcchdo.formats.ctd.exchange as ctdex


def main(argv):
    if len(argv) < 2:
        print "Usage: %s <ctd exchange>" % argv[0]
        return 1
    
    file = libcchdo.model.datafile.DataFile()
    with open(argv[1], 'r') as in_file:
        ctdex.read(file, in_file)

    ctdnc.write(file, sys.stdout)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
