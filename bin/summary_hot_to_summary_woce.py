#!/usr/bin/env python

from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo

if len(sys.argv) < 3:
    print 'Usage:', sys.argv[0], '<HOT sumfile>'
    sys.exit(1)

with open(sys.argv[1], 'r') as in_file:
    file = libcchdo.SummaryFile()
    file.read_HOT_Summary(in_file)
    file.write_WOCE_Summary(sys.stdout)
