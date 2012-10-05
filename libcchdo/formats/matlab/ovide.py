from datetime import datetime

from numpy import ma

from . import *
from libcchdo.formats import woce


def read(self, handle):
    mat = loadmat(handle)

    self.create_columns([
        'STNNBR',
        'CASTNO',
        'SAMPNO',
        'BTLNBR',
        '_DATETIME',
        '_ENDDT',
    ])

    for i, x in enumerate(mat['STATION_NUMBER']):
        self['STNNBR'].set(i, x[0])
        self['CASTNO'].set(i, x[0])
        self['SAMPNO'].set(i, x[0])
        self['BTLNBR'].set(i, x[0])
    date_fmt = '%Y%m%d%H%M%S'
    for i, x in enumerate(mat['STATION_DATE_BEGIN']):
        self['_DATETIME'].set(i, datetime.strptime(x, date_fmt))
    for i, x in enumerate(mat['STATION_DATE_END']):
        self['_ENDDT'].set(i, datetime.strptime(x, date_fmt))

    all_params = sorted(list(set(mat.keys()) - set(NOT_PARAMS)))
    params = []

    for p in all_params:
        fake_p = '_%s' % p
        if p.endswith('_RESP') or p.endswith('_RESP_ORG'):
            values = ''.join(mat[p])
            self.globals[fake_p] = values
        else:
            params.append(p)

    self.create_columns(['_%s' % p for p in params])

    horizontal = [
        'CRUISE_NAME', 'DATA_CENTRE', 'DATA_MODE', 'DIRECTION',
        'INST_REFERENCE', 'PI_NAME', 'POSITIONING_SYSTEM', 'ROSETTE_TYPE',
        'SHIP_NAME', 'SHIP_WMO_ID', 'STATION_DATE_BEGIN', 'STATION_DATE_END',
        'STATION_PARAMETERS', 'STATION_PARAM_CHIM', ]

    param_info = [
        'INST_REFERENCE', 'SHIP_WMO_ID', 'PI_NAME', 'DATA_MODE',
        'STATION_PARAM_CHIM', 'SHIP_NAME', 'DIRECTION', 'CRUISE_NAME',
        'DATA_CENTRE', 'STATION_PARAMETERS', ]

    for p in params:
        fake_p = '_%s' % p
        values = mat[p]

        if p.endswith('_PREC'):
            values = values[:, 0]
        elif p in horizontal:
            pass
        elif p.endswith('_FLAG') or p.endswith('_QC'):
            pass
        else:
            values = values[:, 0]

        # XXX
        # Flags have some character sequence that I haven't figured out the
        # meaning for yet.
        if p.endswith('_FLAG'):
            continue
        for i, x in enumerate(values):
            if x == -9999:
                self[fake_p].set(i, None)
            else:
                self[fake_p].set(i, x)

