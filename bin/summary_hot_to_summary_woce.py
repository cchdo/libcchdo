#!/usr/bin/env python

from __future__ import with_statement
import sys

import implib as L
import implib.model.datafile


def main(argv):
    if len(argv) < 3:
        print 'Usage:', argv[0], '<HOT sumfile>'
        return 1
    
    with open(argv[1], 'r') as in_file:
        file = L.model.datafile.SummaryFile()
        file.read_HOT_Summary(in_file)
        file.write_WOCE_Summary(sys.stdout)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
