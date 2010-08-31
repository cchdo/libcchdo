#!/usr/bin/env python

from __future__ import with_statement

import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.bottle.exchange as botex
import libcchdo.formats.bottle.zip.netcdf as botzipnc


def main(argv):
    if len(argv) < 2:
        print 'Usage: %s <bottle exchange zip>' % argv[0]
        return 1
    
    incoming = libcchdo.DataFile()
    with open(argv[1], 'r') as in_file:
        botex.read(incoming, in_file)

    # TODO split apart the bottle exchange file into a data file collection
    # based on station cast. Each cast is a new "file"

    print incoming

    outgoing = libcchdo.DataFileCollection()
    
    botzipnc.write(outgoing, sys.stdout)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
