#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path, stdout
path.insert(0, '/'.join(path[0].split('/')[:-1]))
import libcchdo

if len(argv) < 2:
  print 'Usage:', argv[0], '<exbot file>'
  exit(1)
file = libcchdo.DataFile()
with open(argv[1], 'r') as in_file:
  file.read_Bottle_Exchange(in_file)
file.write_Google_Wire(stdout)
