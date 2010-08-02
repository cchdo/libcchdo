#!/usr/bin/env python

from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.coriolis.coriolis as coriolis
import libcchdo.formats.bottle.exchange as botex


def main():
    if len(sys.argv) < 2:
        print 'Usage:', sys.argv[0], '<coriolis file>'
        return 1
    
    with open(sys.argv[1], 'r') as in_file:
        file = libcchdo.DataFile()
        coriolis.read(file, in_file)
        botex.write(file, sys.stdout)


if __name__ == '__main__':
    sys.exit(main())
