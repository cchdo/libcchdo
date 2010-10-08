#!/usr/bin/env python

from __future__ import with_statement
import sys

import abs_import_library
import libcchdo.model.datafile
import libcchdo.formats.ctd.zip.exchange as ctdzipex
import libcchdo.formats.ctd.zip.netcdf as ctdzipnc


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<ctd exchange zip>'
        return 1
    
    file = libcchdo.model.datafile.DataFileCollection()
    with open(argv[1], 'r') as in_file:
        ctdzipex.read(file, in_file)
    
    ctdzipnc.write(file, sys.stdout)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
