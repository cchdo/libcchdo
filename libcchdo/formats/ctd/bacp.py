"""Format module for CLIVAR files that have BACp.

Originally noticed as a file that contained BACp (XMISS/TRANSM/beam attenuation)
and CTDPRS. Future files had other parameters in them prompting revisiting as an
actual format type.

"""
from datetime import datetime

from libcchdo.fns import _decimal
from libcchdo.log import LOG


# TODO provide something like an OSVar to map format variables to our variables
VARIABLE_MAP = {
    'Pres': ['CTDPRS', 'DBAR'],
    'T': ['CTDTMP', 'ITS-90'],
    'S': ['CTDSAL', 'PSS-78'],
    'Oxy': ['CTDOXY', ''],
    'BACp': ['TRANSM', ''],
}


def read(self, handle):
    """How to read a CTD BACp file.

    This type of file begins with the characters 'CLIVAR'. Following that is a
    list of parameters in the file separated by comma space.

    """
    header = handle.readline().rstrip()
    if not (header.startswith('CLIVAR') and header.endswith('data')):
        raise ValueError('{0!r} is not a BACp file.'.format(handle))

    stripped_header = header[len('CLIVAR'):-len('data')]
    if stripped_header.startswith(':'):
        stripped_header = stripped_header[1:]
    raw_params = [x.strip() for x in stripped_header.split(',')]
    params = []
    for param in raw_params:
        try:
            params.append(VARIABLE_MAP[param])
        except KeyError:
            LOG.warn(u'Unable to map parameter {0}'.format(param))
            params.append(None)

    sect_id = ' '.join(handle.readline().strip().split()[1:])
    date_tuple = map(int, handle.readline().strip()[len('Date:'):].split())
    time_tuple = map(int, handle.readline().strip()[len('Time '):].split())
    dtime = datetime(*(date_tuple + time_tuple))
    lat = handle.readline().split(':')[1].strip()
    lng = handle.readline().split(':')[1].strip()
    depth = handle.readline().split(':')[1].strip()
    station = handle.readline().split(':')[1].strip()
    cast = handle.readline().split(':')[1].strip()

    self.globals['SECT_ID'] = sect_id
    self.globals['STNNBR'] = station
    self.globals['CASTNO'] = cast
    self.globals['_DATETIME'] = dtime
    self.globals['LATITUDE'] = lat
    self.globals['LONGITUDE'] = lng
    self.globals['DEPTH'] = depth

    columns, units = zip(*params)

    self.create_columns(columns, units)

    for l in handle:
        for i, v in enumerate(map(_decimal, l.split())):
            self[columns[i]].append(v)

    self.check_and_replace_parameters()
