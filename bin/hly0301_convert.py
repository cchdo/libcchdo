#!/usr/bin/env python


from __future__ import with_statement
import datetime
import os
import sys

sys.path.insert(0, '/Users/myshen/Documents/libcchdo/bin')

import abs_import_library
from libcchdo import LOG
from libcchdo import config
from libcchdo.formats import add_pre_write
from libcchdo.model.datafile import DataFileCollection
from libcchdo.formats.ctd.zip import exchange as ctdzipex
from libcchdo.units import convert as ucvt


def operate_healy_file(df, stamp):
    LOG.info('Attaching unit converters')
    cvt = ucvt.ctdoxy_micromole_per_liter_to_micromole_per_kilogram
    df.unit_converters[('UMOL/L', 'UMOL/KG')] = cvt
    df.unit_converters[('MMOLE/M^3', 'UMOL/KG')] = cvt
    df.unit_converter_technique[cvt] = '(MMOL/M^3)/(1 + CTDSIGTH/1000)'

    def change_stamp(self):
        self.globals['stamp'] = stamp

    add_pre_write(change_stamp)

    df.check_and_replace_parameters()

    # Remove columns
    LOG.info('Removing unwanted columns')
    unwanted = ('CTDDEP CTDPOTTMP CTDSIGTH CTDOXYV CTDOXPCSAT '
                'CTDOXSAT CTDNOBS').split()
    deleted = []
    for c in unwanted:
        if df[c]:
            del df[c]
            deleted.append(c)
    if deleted:
        df.changes_to_report.append('Removed columns: %s' % ', '.join(deleted))

    # Change expocode
    LOG.info('Changing expocode')
    expocode = '32H120030721'
    df.globals['EXPOCODE'] = expocode
    df.changes_to_report.append('Changed EXPOCODE from HLY031 to 32H120030721')

    # Add flag 2 to all columns
    LOG.info('Adding QC flags')
    for c in df.columns.values():
        c.flags_woce = [2] * len(c)


def main(argv):
    if len(argv) < 2:
        print "Usage: <healy zip file>"
        return 1

    dfc = DataFileCollection()
    stamp = config.stamp()
    with open(argv[1], 'r') as infile:
        ctdzipex.read(dfc, infile, retain_order=True)
    for f in dfc.files:
        operate_healy_file(f, stamp)
    ctdzipex.write(dfc, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
