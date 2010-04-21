#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path, stdout
path.insert(0, '/'.join(path[0].split('/')[:-1]))

from libcchdo import DataFileCollection
from formats.ctd.zip.exchange import exchange
from formats.common.nav import nav

if len(argv) < 2:
    print 'Usage:', argv[0], '<CTDZip Exchange file>'
    exit(1)

file = DataFileCollection()
with open(argv[1], 'r') as in_file:
    exchange(file).read(in_file)
    nav(file).write(stdout)
