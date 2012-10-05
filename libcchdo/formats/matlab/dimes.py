"""DIMES high-resolution profiler Matlab format tools.

"""

from . import *


def read(self, handle, global_params, vertical_params):
    """Read generic DIMES matlab file."""
    mat = loadmat(handle)

    self.globals['_matlab_file_header'] = mat['__header__']
    self.globals['_matlab_file_version'] = mat['__version__']

    for gp in global_params:
        self.globals[global_params[gp]] = mat[gp][0, 0]

    params = sorted(
        list(set(mat.keys()) - set(NOT_PARAMS) - set(global_params.keys())))

    self.create_columns(params)
    for p in params:
        col = self[p]
        if p in vertical_params:
            l = mat[p][:, 0]
        else:
            l = mat[p][0]
        for i, x in enumerate(l):
            col.set(i, x)
