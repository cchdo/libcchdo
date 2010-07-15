#!/usr/bin/env python

from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.google_wire.google_wire as google_wire

if len(sys.argv) < 2:
    print 'Usage:', sys.argv[0], '<any recognized CCHDO file>'
    sys.exit(1)

with open(sys.argv[1], 'r') as infile:
    file = libcchdo.fns.read_arbitrary(infile)
    google_wire.write(file, sys.stdout)
