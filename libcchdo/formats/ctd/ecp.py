from datetime import datetime
from libcchdo.fns import ddm_to_dd, _decimal, equal_with_epsilon


def read(self, handle):
    """How to read CTD Bonus Goodhope files from a TAR."""
    lines = handle.readlines()

    line0 = lines[0].split()

    sect_id = line0[1]
    station = str(int(line0[0]))

    line3 = lines[3].split()
    lattoks = [line3[3], line3[4], line3[2]]
    lontoks = [line3[6], line3[7], line3[5]]
    try:
        latitude = ddm_to_dd(lattoks)
    except ValueError:
        latitude = ddm_to_dd([lattoks[1], lattoks[2], lattoks[0]])
    try:
        longitude = ddm_to_dd(lontoks)
    except ValueError:
        longitude = ddm_to_dd([lontoks[1], lontoks[2], lontoks[0]])
    date = line3[0]
    time = line3[1].zfill(4)
    depth = line3[10]

    self.globals['EXPOCODE'] = None
    self.globals['SECT_ID'] = sect_id
    self.globals['STNNBR'] = station
    self.globals['CASTNO'] = '1'
    self.globals['LATITUDE'] = latitude
    self.globals['LONGITUDE'] = longitude
    self.globals['DEPTH'] = depth
    self.globals['_DATETIME'] = datetime.strptime(date + time, '%d%m%Y%H%M')

    param_units = [
        ['CTDPRS', 'DBAR'],
        ['CTDTMP', 'ITS-90'],
        ['CTDSAL', 'PSS-78'],
        ['CTDOXY', 'UMOL/KG'],
        ['THETA', 'DEG C'],
        ['DEPTH', 'METERS'],
        ['SIG0', 'KG/M^3'],
        ['GAMMA', 'KG/M^3'],
    ]

    columns = []
    units = []
    for p, u in param_units:
        columns.append(p)
        units.append(u)

    self.create_columns(columns, units)

    data = lines[14:]
    for l in data:
        for i, v in enumerate(map(float, l.split())):
            v = _decimal(v)
	    flag_woce = 2
	    if equal_with_epsilon(v, 9.0):
                v = None
                flag_woce = 9
            self[columns[i]].append(v, flag_woce=flag_woce)

    self.check_and_replace_parameters()
