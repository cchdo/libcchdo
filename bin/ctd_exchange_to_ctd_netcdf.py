#!/usr/bin/env python

from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.ctd.netcdf as ctdnc
import libcchdo.formats.ctd.exchange as ctdxchg

if len(sys.argv) < 2:
    print >> sys.stderr, "Usage: %s <ctd_exchange>" % sys.argv[0]
    sys.exit(1)

file = libcchdo.DataFile()
with open(sys.argv[1], "rb") as infile:
    ctdxchg.read(file, infile)

ctdnc.write(file, sys.stdout)
