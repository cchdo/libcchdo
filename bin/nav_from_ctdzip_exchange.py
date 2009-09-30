#!/usr/bin/env python

from __future__ import with_statement
from .. import libcchdo
from sys import argv, exit

if len(argv) < 2:
  print 'Usage:', argv[0], '<CTDZip Exchange file>'
  exit(1)
file = libcchdo.DataFileCollection()
with open(argv[1], 'r') as in_file:
  file.read_CTDZip_Exchange(in_file)
file.write_nav(sys.stdout)
