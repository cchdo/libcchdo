#!/usr/bin/env python

from __future__ import with_statement
import sys

import implib as L
import implib.model.datafile
import implib.formats.coriolis.coriolis as coriolis
import implib.formats.bottle.exchange as botex


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<coriolis file>'
        return 1
    
    with open(argv[1], 'r') as in_file:
        file = L.model.datafile.DataFile()
        coriolis.read(file, in_file)
        botex.write(file, sys.stdout)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
