from ...model import datafile


def read(meta, filename):
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

    datafile = datafile.DataFile()

    preamble = """\
# Auto-generated Exchange CTD file from ctd_polarstern_to_ctd_exchange
# Please verify integrity before use.
#
# Original data acquired from CD
# Reference website: http://www.awi.de/en/research/research_divisions/climate_science/observational_oceanography
#
"""

    citation = "# Citation: %s (%d)\n#     %s\n" % (
            meta["cites"]["name"],
            meta["cites"]["year"],
            meta["cites"]["description"])
    reference = "# Reference(s): %s (%d)\n#      %s\n" % (
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

    datafile.globals['header'] = preamble + citation + reference + parameter_descriptions

    datafile.globals["EXPOCODE"] = None
    datafile.globals["SECT"] = meta["events"]["campaign"]

    cruise = None
    cast_info = None
    stn_cast = meta["events"]["name"].split(" ")[0]

    try:
        cruise, cast_info = stn_cast.split("/")
    except ValueError:
        print >> sys.stderr, "ValueError for", filename, "with station and cast info '%s'" % stn_cast
        cruise = meta["events"]["name"]
        cast_info = "000-0"

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

        for param in parameters:
            col = None

            if param not in PARAM_EQUIVS:
                final_params.append(None)
                continue

            final_params.append(PARAM_EQUIVS[param])
            col = datafile.Column(PARAM_EQUIVS[param])
            col.parameter.units = meta[param]["units"] if \
                    meta[param]["units"] else \
                    col.parameter.units

            datafile.columns[PARAM_EQUIVS[param]] = col

        for line in file:
            values = map(lambda x: x.strip(), line.split("\t"))
            for datum, param in zip(values, final_params):
                if not param:
                    continue
                if not datum or datum == '':
                    datafile.columns[param].values.append(-999.0)
                else:
                    datafile.columns[param].values.append(float(datum))

    return datafile



