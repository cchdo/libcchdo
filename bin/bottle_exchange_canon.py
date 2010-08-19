#!/usr/bin/env python

from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.bottle.exchange as botex

if len(sys.argv) < 2:
    print 'Usage:', sys.argv[0], '<botex file>'
    sys.exit(1)

with open(sys.argv[1], 'r') as in_file:
    file = libcchdo.DataFile(allow_contrived=True)
    botex.read(file, in_file)
    botex.write(file, sys.stdout)
