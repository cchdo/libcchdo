#!/usr/bin/env python


from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.google_wire.google_wire as google_wire


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<any recognized CCHDO file> [json(true)]'
        return 1

    json = len(argv) > 2 and argv[2].lower() == 'true'
    
    with open(argv[1], 'r') as in_file:
        file = libcchdo.fns.read_arbitrary(in_file)
        google_wire.write(file, sys.stdout, json=json)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
