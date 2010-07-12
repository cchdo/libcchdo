#!/usr/bin/env python

from __future__ import with_statement

import sqlite3
import sys
sys.path.insert(0, "/".join(sys.path[0].split("/")[:-2]))
import os
import os.path
import datetime

import libcchdo
import libcchdo.formats.ctd.exchange as exctd


RED = "\x1b\x5b\x31;31m"
GREEN = "\x1b\x5b\x31;32m"
YELLOW = "\x1b\x5b\x31;33m"
CYAN = "\x1b\x5b\x30;36m"
BOLD = "\x1b\x5b\x31;37m"
CLEAR = "\x1b\x5b\x30m"


def read_ctd_polarstern(meta, filename):
    PARAM_EQUIVS = {
        #"param_depth_water": None,
        "param_press": "CTDPRS",
        "param_sal": "CTDSAL",
        #"param_sigma_theta": None,
        "param_temp": "CTDTMP",
        "param_tpot": "THETA",
        #"param_cond": "CTDCOND", # FIXME
        "param_nobs": "CTDNOBS",
        #"param_atten": "XMISS",
        #"param_ys_fl": None,
        "param_chl_fluores": "FLUOR", # FIXME
    }

    datafile = libcchdo.DataFile()

    preamble = """\
# Auto-generated Exchange CTD file from ctd_polarstern_to_ctd_exchange
# Please verify integrity before use.
#
# Original data acquired from CD
# Reference website: http://www.awi.de/en/research/research_divisions/climate_science/observational_oceanography
#
"""

    citation = "# Citation: %s (%d): %s\n" % (
            meta["cites"]["name"],
            meta["cites"]["year"],
            meta["cites"]["description"])
    reference = "# Reference(s): %s (%d): %s\n" % (
            meta["cites"]["name"],
            meta["cites"]["year"],
            meta["cites"]["description"])
    parameter_descriptions = "# Parameters\n"
    for attr in meta:
        if "param" in attr and attr in PARAM_EQUIVS:
            parameter_descriptions += "#   %s (%s) [%s]: %s * %s (%s)\n" % (
                    PARAM_EQUIVS[attr],
                    attr[6:],
                    meta[attr]["units"],
                    meta[attr]["method"],
                    meta[attr]["comment"],
                    meta[attr]["pi"])

    datafile.header = preamble + citation + reference + parameter_descriptions

    datafile.globals["EXPOCODE"] = None
    datafile.globals["SECT"] = meta["events"]["campaign"]
    cruise, cast_info = meta["events"]["name"].split("/")
    if len(cast_info.split("-")) == 1:
        datafile.globals["STNNBR"] = cast_info
        datafile.globals["CASTNO"] = "1"
    else:
        datafile.globals["STNNBR"], datafile.globals["CASTNO"] = \
                cast_info.split("-")
    date_time = datetime.datetime.strptime(
            meta["events"]["date_time"].upper(),
            "%Y-%m-%dT%H:%M:%S")
    datafile.globals["DATE"] = date_time.strftime("%Y%m%d")
    datafile.globals["TIME"] = date_time.strftime("%H%M")
    datafile.globals["LATITUDE"] = meta["events"]["latitude"]
    datafile.globals["LONGITUDE"] = meta["events"]["longitude"]
    datafile.globals["DEPTH"] = meta["max_depth"]

    with open(filename, "rb") as file:

        # skip the metadata; we read that already (in database; arg)
        while "*/" not in file.readline():
            pass

        def prepare_parameter(param):
            s = param.lower()[:param.find("\x5b")].strip() if \
                param.find("\x5b") != -1 else \
                param.lower().strip()
            return "param_" + s.replace(" ", "_").replace("-", "_")

        parameters = map(prepare_parameter, file.readline().split("\t"))

        final_params = []
        ignored_params= []

        for param in parameters:
            col = None

            if param not in PARAM_EQUIVS:
                ignored_params.append(param)
                continue

            final_params.append(PARAM_EQUIVS[param])
            col = libcchdo.Column(PARAM_EQUIVS[param])
            col.parameter.units = meta[param]["units"] if \
                    meta[param]["units"] else \
                    col.parameter.units

            datafile.columns[PARAM_EQUIVS[param]] = col

        for line in file:
            values = line.split("\t")
            for datum, param in zip(values, parameters):
                if param in ignored_params:
                    continue
                datafile.columns[PARAM_EQUIVS[param]].values.append(float(datum))

    return datafile


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

            datafile = read_ctd_polarstern(meta, input_filename)

            if COMMIT_TO_FILE:
                with open(output_filename, "wb") as output_file:
                    exctd.write(datafile, output_file)
            else:
                print "%sOutput to %s (not written):%s" % (BOLD, output_filename, CLEAR)
                exctd.write(datafile, sys.stdout)

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
