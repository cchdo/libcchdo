#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path, stdout
path.insert(0, '/'.join(path[0].split('/')[:-1]))
from libcchdo import DataFile
from formats.bottle.exchange import exchange
from formats.google_wire.google_wire import google_wire

if len(argv) < 2:
  print 'Usage:', argv[0], '<exbot file>'
  exit(1)
with open(argv[1], 'r') as in_file:
  file = DataFile(allow_contrived=True)
  exchange(file).read(in_file)
  google_wire(file).write(stdout)
