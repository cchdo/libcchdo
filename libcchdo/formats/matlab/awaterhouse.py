"""High-resolution profiler Matlab format tools.

"""

from logging import getLogger
from collections import OrderedDict

from math import isnan

import numpy as np

from libcchdo.fns import _decimal
from libcchdo.model.datafile import DataFile
from . import *


log = getLogger(__name__)


def hrp_data_as_dict(hrp):
    """Convert an HRP ndarray into a dictionary of param names to ndarrays."""
    ddd = OrderedDict()
    params = hrp.dtype.names
    data = hrp[0, 0]
    for iii, param in enumerate(params):
        ddd[param] = data[iii]
    return ddd


def ndarray_data_slice(ndarray):
    """Discover the slice that has data."""
    notnan = np.where(np.isnan(ndarray) == False)
    try:
        return (notnan[0][0], notnan[0][-1])
    except IndexError:
        return (0, -1)


def is_data_range_inside(drange, ref_range):
    """Warn if the data range is outside the reference range."""
    if drange[0] < ref_range[0]:
        return False
    if drange[1] > ref_range[1]:
        return False
    return True


def load_mat_hrp(fileobj):
    mat = loadmat(fileobj)

    log.debug(u'matlab file header: {0}'.format(mat['__header__']))
    log.debug(u'matlab file version: {0}'.format(mat['__version__']))

    data_key = "hrp"
    try:
        hrp = mat[data_key]
    except KeyError:
        possible_keys = mat.keys()
        for key in reversed(possible_keys):
            if key.startswith('__') and key.endswith('__'):
                possible_keys.remove(key)
        if len(possible_keys) == 1:
            data_key = possible_keys[0]
            log.info(u'Using non-standard key for matlab: {0}'.format(data_key))
            hrp = mat[data_key]
        else:
            log.error(u'Unable to determine key for matlab. Possible keys: '
                      '{0}'.format(possible_keys))
            raise
    return mat, hrp


def read(dfc, fileobj, cfg):
    """Read generic HRP matlab file."""
    mat, hrp = load_mat_hrp(fileobj)
    data = hrp_data_as_dict(hrp)

    coords = zip(data['lon'][0], data['lat'][0])
    del data['lat']
    del data['lon']

    for coord in coords:
        dfile = DataFile()
        dfc.append(dfile)
        dfile.globals['LONGITUDE'] = _decimal(coord[0])
        dfile.globals['LATITUDE'] = _decimal(coord[1])

        dfile.create_columns(data.keys())

    for key in data.keys():
        log.info(u'parameter shape: {0} {1}'.format(key, data[key].shape))

    param_map = cfg["parameter_mapping"]
    for param in data.keys():
        if param not in param_map:
            del data[param]
        else:
            new_key = param_map[param]
            old_key = data[param]
            if new_key != old_key:
                data[new_key] = data[old_key]
                del data[old_key]

    for dep, dfile in enumerate(dfc):
        dfile.globals['STNNBR'] = dep + 1
        ref_range = ndarray_data_slice(data['PRESSURE'][:, dep])
        for param, pdata in data.items():
            col = dfile[param]
            data_col = pdata[:, dep]

            drange = ndarray_data_slice(data_col)
            if ref_range is None:
                ref_range = drange
                determiner = param
            elif drange != ref_range:
                if drange[0] == drange[1]:
                    log.info(u'No data for {0}. Skip.'.format(param))
                    continue
                if not is_data_range_inside(drange, ref_range):
                    log.error(u'{0} has data range {1} outside {2}. '
                              'Skip.'.format(param, drange, ref_range))
                    continue

            col.values = map(_decimal,
                             list(data_col[ref_range[0]:ref_range[1]]))
            # Act as if all files had QC and assign it to OceanSITES 1. Assuming
            # that someone has already gone through level 0 data and we are
            # receiving level 1 or higher. We can set all flags to 2.
            col.flags_woce = [9 if isnan(val) else 2 for val in col.values]

    # Somehow, HRP matlab data can have nans in the coordinate arrays. We can't
    # recalculate depth from that or make other assumptions so we can only
    # delete them.
    for iii, dfile in reversed(list(enumerate(dfc))):
        if (isnan(dfile.globals['LATITUDE']) or
            isnan(dfile.globals['LONGITUDE'])):
            log.warn(u'Unable to determine coordinate for matlab row '
                     '{0}. Discarding.'.format(iii))
            dfc.files.remove(dfile)
