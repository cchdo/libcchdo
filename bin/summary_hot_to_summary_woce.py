#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path, stdout
path.insert(0, '/'.join(path[0].split('/')[:-1]))
from libcchdo import SummaryFile

if len(argv) < 3:
  print 'Usage:', argv[0], '<HOT sumfile>'
  exit(1)
file = SummaryFile()
with open(argv[1], 'r') as in_file:
  file.read_HOT_Summary(in_file)
file.write_WOCE_Summary(stdout)
