#!/usr/bin/env python


from __future__ import with_statement
import getopt
import sys

import abs_import_library
import libcchdo.formats.google_wire.google_wire as google_wire


def main(argv):
    opts, args = getopt.getopt(argv[1:], 'j', ['json'])

    if len(args) < 1:
        print 'Usage:', argv[0], '[-j|--json] <any recognized CCHDO file>'
        return 1

    # XXX hidden option: if program is supplied two args with the last as true
    # the output format is selected to be json
    json = len(args) > 1 and args[1].lower() == 'true'

    for o, a in opts:
        if o in ('-j', '--json'):
            json = True
        else:
            assert False, "unhandled option"

    with open(args[0], 'r') as in_file:
        file = libcchdo.fns.read_arbitrary(in_file)
        google_wire.write(file, sys.stdout, json=json)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
