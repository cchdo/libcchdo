#!/usr/bin/env python


from __future__ import with_statement
import sys

import abs_import_library
import libcchdo.fns
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
