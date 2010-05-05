#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path, stdout
path.insert(0, '/'.join(path[0].split('/')[:-1]))

import libcchdo
import formats.ctd.zip.exchange as ctdzipex
import formats.common.nav as nav

if len(argv) < 2:
    print 'Usage:', argv[0], '<CTDZip Exchange file>'
    exit(1)

with open(argv[1], 'r') as in_file:
    file = libcchdo.DataFileCollection()
    ctdzipex.read(file, in_file)
    nav.write(file, stdout)
