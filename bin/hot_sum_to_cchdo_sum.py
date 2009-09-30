#!/usr/bin/env python

from __future__ import with_statement
import libcchdo

from sys import argv, exit

if len(argv) < 3:
  print 'Usage:', argv[0], '<HOT sumfile> <output CCHDO sumfile> [nav file output]'
  exit(1)
file = libcchdo.SummaryFile()
with open(argv[1], 'r') as in_file:
  file.read_HOT(in_file)
with open(argv[2], 'w') as out_file:
  file.write_WOCE_Summary(out_file)
if len(argv) is 4:
  with open(argv[3], 'w') as out_file:
    file.write_nav(out_file)
