#!/usr/bin/env python


from __future__ import with_statement
import sys

import implib as L
import implib.fns


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<any recognized CCHDO file>'
        return 1
    
    with open(argv[1], 'r') as in_file:
        file = L.fns.read_arbitrary(in_file)
        print file.to_dict()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
