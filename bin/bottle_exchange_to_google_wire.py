#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path, stdout
path.insert(0, '/'.join(path[0].split('/')[:-1]))

from libcchdo import DataFile
import formats.bottle.exchange as exchange
import formats.google_wire.google_wire as google_wire

if len(argv) < 2:
    print 'Usage:', argv[0], '<exbot file>'
    exit(1)

with open(argv[1], 'r') as in_file:
    file = DataFile(allow_contrived=True)
    exchange.read(file, in_file)
    google_wire.write(file, stdout)
