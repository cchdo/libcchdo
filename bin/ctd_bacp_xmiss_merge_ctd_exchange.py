#!/usr/bin/env python

from __future__ import with_statement
import sys

import implib as L
import implib.model.datafile
import implib.formats.ctd.bacp as ctdbacp
import implib.formats.ctd.exchange as ctdex


PRESSURE_COLUMNS = ('CTDPRS', 'CTDRAW', )


def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, "Usage: %s <ctd_bacp> <ctd_exchange>" % argv[0]
        return 1

    if len(argv) < 3:
        print >> sys.stderr, "Usage: %s <ctd_bacp> <ctd_exchange>" % argv[0]
        return 1
    
    mergefile = L.model.datafile.DataFile()
    file = L.model.datafile.DataFile()

    with open(argv[1], "r") as infile:
        ctdbacp.read(mergefile, infile)

    with open(argv[2], 'r') as infile:
        ctdex.read(file, infile)

    merge_pressure = None
    pressure = None
    for c in PRESSURE_COLUMNS:
        try:
            merge_pressure = mergefile.columns[c]
            pressure = file.columns[c]
        except KeyError:
            pass
    if merge_pressure is None or pressure is None:
        print >> sys.stderr, ('Unable to find a matching pressure column in '
                              'both files. Could not merge.')
        return 1

    xmiss_column = None
    try:
        xmiss_column = file.columns['XMISS']
    except KeyError:
        pass
    if not xmiss_column:
        xmiss_column = file.columns['XMISS'] = L.model.datafile.Column('XMISS')
        xmiss_column.values = [None] * len(file)

    merge_xmiss = None
    try:
        merge_xmiss = mergefile.columns['XMISS']
    except KeyError:
        pass
    if not merge_xmiss:
        print >> sys.stderr, ('Merge file has no XMISS column to merge')
        return 1

    for i, p in enumerate(merge_pressure.values):
        j = pressure.values.index(p)
        xmiss_column.values[j] = merge_xmiss.values[i]

    ctdex.write(file, sys.stdout)

if __name__ == '__main__':
    sys.exit(main(sys.argv))
