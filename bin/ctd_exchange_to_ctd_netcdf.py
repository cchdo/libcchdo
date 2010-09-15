#!/usr/bin/env python

from __future__ import with_statement
import sys

import abs_import_libcchdo
import libcchdo.formats.ctd.netcdf as ctdnc
import libcchdo.formats.ctd.exchange as ctdxchg


def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, "Usage: %s <ctd_exchange>" % argv[0]
        sys.exit(1)
    
    file = libcchdo.DataFile()
    with open(argv[1], "rb") as infile:
        ctdxchg.read(file, infile)

    ctdnc.write(file, sys.stdout)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
