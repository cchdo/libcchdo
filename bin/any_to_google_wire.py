#!/usr/bin/env python


from __future__ import with_statement
import getopt
import sys

import abs_import_library
import libcchdo.fns
from libcchdo.formats.google_wire import google_wire


def main(argv):
    opts, args = getopt.getopt(argv[1:], 'jh', ['json', 'help'])
    usage = "Usage: %s [-j|--json] <any recognized CCHDO file>" % argv[0]

    if len(args) < 1:
        print >> sys.stderr, usage
        return 1

    flag_json = False

    for o, a in opts:
        if o in ('-j', '--json'):
            flag_json = True
        elif o in ('-h', '--help'):
        	print >> sys.stderr, usage
        	return 1
        else:
            assert False, "unhandled option"

    with open(args[0], 'r') as in_file:
        file = libcchdo.fns.read_arbitrary(in_file)
        google_wire.write(file, sys.stdout, json=flag_json)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
