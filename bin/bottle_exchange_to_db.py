#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path
path.insert(0, '/'.join(path[0].split('/')[:-1]))
import libcchdo
from formats.bottle.database import database
from formats.bottle.exchange import exchange

if len(argv) < 2:
  print 'Usage:', argv[0], '<exbot file>'
  exit(1)
with open(argv[1], 'r') as in_file:
  file = libcchdo.DataFile()
  exchange(file).read(in_file)
  database(file).write()
