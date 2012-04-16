""" Plotting functions

Miscellaneous useful matplotlib.basemap functions

"""


def sqdist(a, b):
    """ Squared distance to avoid square root overhead when calculating distances

    """
    return (b[0] - a[0]) ** 2 + (b[1] - a[1]) ** 2


def vertex_reduce(pl, tol=1):
    """ Reduces the number of vertices of a linestring by combining sufficiently
        similar vertices

        Parameters:
            pl - a polyline [(x0, y0), (x1, y1), ...]
            tol - the approximation tolerance

        Implementation of pseudo-code:
        http://softsurfer.com/Archive/algorithm_0205/algorithm_0205.htm
    """
    sqtol = tol ** 2

    last = None
    reduced = []
    for v in pl:
        if last is None or sqdist(v, last) > sqtol:
            reduced.append(v)
            last = v
    return reduced


def draw_graticules(map, spacing_meridians=5, spacing_parallels=5):
    """ Draws graticules on the map

        The spacing will be every 5 degrees and labels will only be on the left
        and bottom. The labels will be done in decimal numbers with ranges -180
        to 180 and -90 to 90.
    """
    map.drawmeridians(np.arange(map.llcrnrlon, map.urcrnrlon, spacing_meridians),
                      labels=[0, 0, 0, 1], labelstyle='+/-')
    map.drawparallels(np.arange(map.llcrnrlat, map.urcrnrlat, spacing_parallels),
                      labels=[1, 0, 0, 0], labelstyle='+/-')


def draw_track_line(map, xs, ys):
    """ Draws a track line on the map

        The track dots will be red and the line will be black.
    """
    map.plot(xs, ys, 'k.-', markerfacecolor='r')
