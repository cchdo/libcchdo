import numpy as np
from logging import getLogger


log = getLogger(__name__)


from libcchdo.util import memoize
from libcchdo.recipes.lru import lru_cache


class BicubicConvolution:
    """ Evaluates a bicubic spline interpolation implemented as a convolution

        Nothing is calculated before-hand so this uses much less memory than
        anything in scipy.interpolate.

        This should be handy for large datasets where pre-calculating splines is
        very expensive.

        WARNING: This implementation is currently very magically bound with
        etopo plotting. It may not work with generic applications.

    """
    def __init__(self, xs, ys, zs):
        """
            xs, ys - 1D arrays indexing into zs
            zs     - 2D matrix of values to be interpolated
        """
        self.xs = xs
        self.ys = ys
        self.zs = zs

    #bicubic_matrix = np.matrix([
    #    [0, 2, 0, 0],
    #    [-1, 0, 1, 0],
    #    [2, -5, 4, -1],
    #    [-1, 3, -3, 1],
    #])

    def interp_cubic(self, p, x):
        return p[1] + 0.5 * x * (
            p[2] - p[0] + x * (
                2. * p[0] - 5. * p[1] + 4. * p[2] - p[3] + x * (
                    3. * (p[1] - p[2]) + p[3] - p[0])))
        #return (0.5 * np.matrix([1, x, x ** 2, x ** 3]) * \
        #    self.bicubic_matrix * np.matrix(p).T)[0,0]

    def interp_bicubic(self, ps, x, y):
        interp_cubic = self.interp_cubic
        return interp_cubic([interp_cubic(p, y) for p in ps], x)

    @memoize
    def size(self):
        return len(self.zs[0]), len(self.zs)

    def get_ni_nj(self, sizex, sizey, ni, nj):
        """ Get one cell of the submatrix around x, y while accounting for data
            wrapping at edges

        """
        # Anything above or below the matrix is reflected back into the data
        if nj < 0:
            nj *= -1
        if nj >= sizey:
            nj = sizey - 2 - (nj % sizey)

        # Anything left or right of the matrix is wrapped back into the data
        ni %= sizex

        # zs passed in are y, x
        return self.zs[nj][ni]

    @lru_cache(720)
    def get_submatrix(self, x, y):
        sizex, sizey = self.size()

        # The submatrix starts at x - 2, y - 2
        base_x = x - 2
        base_y = y - 2

        # If the submatrix will be entirely inside the data, just get it
        if ((0 < base_x and base_x < sizex - 4) and 
            (0 < base_y and base_y < sizey - 4)):
            return self.zs[base_y:base_y + 4, base_x:base_x + 4].tolist()

        r = range(4)
        return [
            [self.get_ni_nj(sizex, sizey, base_x + i, base_y + j) for j in r]
            for i in r
        ]

    def get_value(self, x, y):
        ix = np.searchsorted(self.xs, x)
        iy = np.searchsorted(self.ys, y)

        # nearest value available in given coordinates
        ax = self.xs[ix]
        ay = self.ys[iy]

        return self.interp_bicubic(self.get_submatrix(ix, iy), 1 - ax + x, 1 - ay + y)

    def __call__(self, xs, ys):
        """ Interpolates the given grid at the newly given coordinates

        """
        zs = []
        for y in ys:
            log.debug('interp y: %f' % y)
            zs.append([self.get_value(x, y) for x in xs])
        return zs
