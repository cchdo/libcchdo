#!/usr/bin/env python

from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import formats.common.nav as nav
import formats.summary.woce as wocesum

if len(sys.argv) < 2:
    print 'Usage:', sys.argv[0], '<WOCE Summary file>'
    sys.exit(1)

with open(sys.argv[1], 'r') as in_file:
    file = libcchdo.SummaryFile()
    wocesum.read(file, in_file)
    nav.write(file, sys.stdout)
