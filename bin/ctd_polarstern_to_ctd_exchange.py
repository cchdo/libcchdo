#!/usr/bin/env python

from __future__ import with_statement

import sqlite3
import sys
import os
import datetime

import implib as L
import implib.formats.ctd.exchange as ctdex
import implib.formats.ctd.polarstern as ctd_polarstern


COMMIT_TO_FILE = True


def main():
    global COMMIT_TO_FILE

    PARAMETERS = {
        7: "param_depth_water",
        8: "param_press",
        9: "param_sal",
        10: "param_sigma_theta",
        11: "param_temp",
        12: "param_tpot",
        13: "param_cond",
        14: "param_nobs",
        15: "param_atten",
        16: "param_ys_fl",
        17: "param_chl_fluores",
    }

    def unpack_param_meta(meta_param):
        return {"units": meta_param[1],
                "pi": meta_param[2],
                "method": meta_param[3],
                "comment": meta_param[4]}

    def unpack_citation(meta_cite):
        return {"name": meta_cite[1],
                "year": meta_cite[2],
                "description": meta_cite[3]}

    def unpack_reference(meta_ref):
        return unpack_citation(meta_ref)

    def unpack_events(meta_events):
        return {"name": meta_events[1],
                "latitude": meta_events[2],
                "longitude": meta_events[3],
                "elevation": meta_events[4],
                "date_time": meta_events[5],
                "location": meta_events[6],
                "campaign": meta_events[7],
                "basis": meta_events[8],
                "device": meta_events[9]}

    db = sqlite3.connect(sys.argv[1])
    # Make sure this database can read its Unicode entries
    db.text_factory = str

    try:
        for input_filename in sys.argv[2:]:

            print input_filename

            meta = {}
            meta_cast = db.cursor().execute(
                    "select * from ctd_casts where filename = ? limit 1",
                    (os.path.basename(input_filename), )).fetchone()

            if not meta_cast:
                print >> sys.stderr, "no metadata for %s" % input_filename
                continue

            meta["filename"] = meta_cast[1]

            meta["cites"] = unpack_citation(db.cursor().execute(
                    "select * from ctd_citations where id = ? limit 1",
                    (meta_cast[2], )).fetchone())

            meta["refs"] = unpack_reference(db.cursor().execute(
                    "select * from ctd_references where id = ? limit 1",
                    (meta_cast[3], )).fetchone())

            meta["events"] = unpack_events(db.cursor().execute(
                    "select * from ctd_events where id = ? limit 1",
                    (meta_cast[4], )).fetchone())

            meta["min_depth"] = meta_cast[5]

            meta["max_depth"] = meta_cast[6]

            for i in PARAMETERS:
                if meta_cast[i] != 0:
                    meta_param = db.cursor().execute(
                            "select * from ctd_%s where id = ? limit 1" %
                            PARAMETERS[i], (meta_cast[i], )).fetchone()
                    meta[PARAMETERS[i]] = unpack_param_meta (meta_param)

            output_filename = os.path.basename(input_filename)
            output_filename = output_filename[:output_filename.find('.')] + \
                              "_ct1.csv"

            datafile = ctd_polarstern.read(meta, input_filename)

            if COMMIT_TO_FILE:
                with open(output_filename, "wb") as output_file:
                    try:
                        ctdex.write(datafile, output_file)
                    except TypeError:
                        print >> sys.stderr, input_filename, \
                                map(lambda col: col.parameter.display_order,
                                datafile.columns.values())
            else:
                print "%sOutput to %s (not written):%s" % ("", output_filename, "")

    finally:
        db.close()


def ensure_args():
    USAGE = """\
usage: %s DATABASE FILE [...]
DATABASE        SQLite3 database containing PolarStern metadata
                (previously extracted)
FILE [...]      The PolarStern data file(s) (*.tab -> *.tab.txt)
""" % sys.argv[0]
    if len(sys.argv[1:]) < 1:
        print >> sys.stderr, USAGE
        sys.exit(0)
    elif len(sys.argv[1:]) == 1:
        print >> sys.stderr, "%s: no input files." % sys.argv[0]
        sys.exit(0)
    try:
        db = sqlite3.connect(sys.argv[1])
        db.close()
    except:
        print >> sys.stderr, "%s is not a SQLite3 database." % sys.argv[1]
        sys.exit(1)


if __name__ == "__main__":
    ensure_args()
    main()
