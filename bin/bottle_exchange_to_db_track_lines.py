#!/usr/bin/env python

from __future__ import with_statement
import sys

import implib as L
import implib.formats.common.track_lines as track_lines


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<exchange bottle file>'
        return 1
    
    with open(argv[1], 'r') as in_file:
        file = L.fns.read_arbitrary(in_file)
        track_lines.write(file, sys.stdout)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
