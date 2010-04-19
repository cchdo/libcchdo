#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path, stdout
path.insert(0, '/'.join(path[0].split('/')[:-1]))
from libcchdo import SummaryFile
from formats.common.nav import nav

if len(argv) < 2:
  print 'Usage:', argv[0], '<WOCE Summary file>'
  exit(1)
file = SummaryFile()
with open(argv[1], 'r') as in_file:
  file.read_Summary_WOCE(in_file)
  nav(file).write(stdout)
