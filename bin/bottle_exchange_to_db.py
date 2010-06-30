#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path
path.insert(0, '/'.join(path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.bottle.database as db
import libcchdo.formats.bottle.exchange as botex

if len(argv) < 2:
    print 'Usage:', argv[0], '<exbot file>'
    exit(1)

with open(argv[1], 'r') as in_file:
    file = libcchdo.DataFile()
    botex.read(file, in_file)
    db.write(file)
