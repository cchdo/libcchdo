"""French CSV format.

"""
from datetime import datetime
from re import compile as re_compile
from csv import reader as csv_reader, excel

from libcchdo.log import LOG
from libcchdo.fns import _decimal
from libcchdo.formats.woce import (
    fuse_datetime, woce_lat_to_dec_lat, woce_lng_to_dec_lng)


class FrCSVDialect(excel):
    """The French CSV format uses semicolons as the delimiter."""
    delimiter = ';' 


frparam_to_param = {
    'Date': 'DATE',
    'Time': 'TIME',
    'Latitude': 'LATITUDE',
    'Longitude': 'LONGITUDE',
    'PRES': 'CTDPRS',
    'TEMP': 'CTDTMP',
    'PSAL': 'CTDSAL',
    'DOX2': 'CTDOXY',
    'CF2W': 'CF2W',
    'CF1W': 'CF1W',
    'PHOW': 'PHSPHT',
    'NTAW': 'NITRAT',
    'SLCW': 'SILCAT',
    # DEPTH is actually bottom depth.
    'DEPH': '_DEPTH',
}


frunit_to_unit = {
    'picomole/kg': 'PMOL/KG',
    'micromole/kg': 'UMOL/KG',
    'Celsius degree': 'DEG C',
    'decibar=10000 pascals': 'DBAR',
    'meter': 'METERS',
    'PO4-P': 'UMOL/KG',
    'NO3-N': 'UMOL/KG',
    'SIO4-SI': 'UMOL/KG',
    'P.S.U.': 'PSU',
    'meter/second': 'M/S',
}


frflag_to_woce_flag = {
    0: 0,
    1: 2,
    2: 3,
    3: 3,
    4: 4,
}


FLAG_F = '_FLAG_F'


def read(dfile, fileobj, data_type=None):
    """Read a French CSV file.

    data_type (optional) if given, must be 'bottle' or 'ctd'. This changes the
        columns that are created (Adds BTLNBR for bottle data).

    NOTE: French CSV used for CTD contains all the CTD casts in one file. Split
    them into a DataFileCollection.

    """
    assert data_type is None or data_type in ['bottle', 'ctd']

    reader = csv_reader(fileobj, dialect=FrCSVDialect())

    # Read header line that contains parameters and units. Convert them to WOCE.
    r_param = re_compile('(.*)\s\[(.*)\]')
    params = []
    units = []
    header = reader.next()
    for param in header:
        matches = r_param.match(param)
        unit = None
        if matches:
            param = matches.group(1)
            unit = matches.group(2)
        elif param == 'Flag':
            param = params[-1] + FLAG_F

        try:
            param = frparam_to_param[param]
        except KeyError:
            pass
        params.append(param)
        try:
            unit = frunit_to_unit[unit]
        except KeyError:
            pass
        units.append(unit)

    non_flag_paramunits = []
    for paramunit in zip(params, units):
        if paramunit[0].endswith(FLAG_F):
            continue
        non_flag_paramunits.append(paramunit)

    # Create all the columns.
    dfile.create_columns(*zip(*non_flag_paramunits))
    columns_id = ['EXPOCODE', 'STNNBR', 'CASTNO']
    col_exp, col_stn, col_cast = dfile.create_columns(columns_id)
    if data_type == 'bottle':
        (col_btln,) = dfile.create_columns(['BTLNBR'])
    dfile.check_and_replace_parameters()
            
    # Read data. Flag columns follow immediately after data columns.
    flags = set()
    flag_values = {}
    for rowi, row in enumerate(reader):
        for param, value in zip(params, row):
            if param == 'LATITUDE':
                lattoks = value[1:].split() + [value[0]]
                value = woce_lat_to_dec_lat(lattoks)
            elif param == 'LONGITUDE':
                lngtoks = value[1:].split() + [value[0]]
                value = woce_lng_to_dec_lng(lngtoks)

            if param.endswith(FLAG_F):
                param = param[:-len(FLAG_F)]
                col = dfile[param]

                if value not in flags:
                    flag_values[value] = [param, rowi, col.values[rowi]]
                    flags.add(value)

                if value == '':
                    value = 9
                try:
                    value = int(value)
                    value = frflag_to_woce_flag[value]
                except (ValueError, KeyError):
                    value = 9
                col.set(rowi, col.get(rowi), flag_woce=value)
            else:
                col = dfile[param]
                if value == '' or value is None:
                    col.set(rowi, None)
                else:
                    col.set(rowi, _decimal(value))
    fuse_datetime(dfile)

    # French CSV does not include cast identifying information. Generate that
    # by watching for coordinate changes.
    # While looping through and finding station changes, also populate the
    # bottom depth column from the _DEPTH column by estimating it as the bottom
    # most depth.
    dfile.create_columns(['DEPTH'])

    last_coord = None
    last_dt = None
    last_depths = []
    stnnbr = 0
    castno = 0
    btlnbr = 1
    col_lat = dfile['LATITUDE']
    col_lng = dfile['LONGITUDE']
    col_dt = dfile['_DATETIME']
    col_bot = dfile['DEPTH']
    try:
        col_depth = dfile['_DEPTH']
    except KeyError:
        method, col_depth = dfile.calculate_depths(col_lat[rowi])
    col_depth = [xxx.to_integral_value() if xxx else xxx for xxx in col_depth]
    for rowi in range(len(dfile)):
        coord = (col_lat[rowi], col_lng[rowi])
        # location changed => station change
        if last_coord != coord:
            stnnbr += 1
            castno = 0
            btlnbr = 1
            last_coord = coord
        # time changed => cast changed
        dtime = col_dt[rowi]
        if last_dt != dtime:
            castno += 1
            btlnbr = 1
            if last_depths:
                col_bot.set_length(rowi, max(last_depths))
            last_depths = []
        else:
            # normal measurement row
            btlnbr += 1
        last_dt = dtime
        col_exp.set(rowi, '')
        col_stn.set(rowi, stnnbr)
        col_cast.set(rowi, castno)
        last_depths.append(col_depth[rowi])
        if data_type == 'bottle':
            col_btln.set(rowi, btlnbr)
    col_bot.set_length(len(dfile), col_depth[len(dfile) - 1])

    try:
        del dfile['_DEPTH']
    except KeyError:
        pass
