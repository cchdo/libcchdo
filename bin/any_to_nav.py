#!/usr/bin/env python


from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.bottle.exchange as botex
import libcchdo.formats.common.nav as nav


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<any recognized CCHDO file>'
        return 1

    with open(argv[1], 'r') as in_file:
        file = libcchdo.fns.read_arbitrary(in_file)
        nav.write(file, sys.stdout)

 
if __name__ == '__main__':
    sys.exit(main(sys.argv))
