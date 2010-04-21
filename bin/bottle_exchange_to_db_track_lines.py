#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path
path.insert(0, '/'.join(path[0].split('/')[:-1]))

from libcchdo import DataFile
from formats.bottle.exchange import exchange

if len(argv) < 2:
    print 'Usage:', argv[0], '<exbot file>'
    exit(1)

with open(argv[1], 'r') as in_file:
    file = DataFile()
    exchange(file).read(in_file)
    file.write_track_lines()
