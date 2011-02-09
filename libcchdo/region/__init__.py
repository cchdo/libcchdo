class Location:

    def __init__(self, coordinate, dtime=None, depth=None):
        self.coordinate = coordinate
        self.datetime = dtime
        self.depth = depth
        # TODO nil axis magnitudes should be matched as a wildcard


class Region:

    def __init__(self, name, *locations):
        self.name = name
        self.locations = locations

    def include (location):
        raise NotImplementedError # TODO

BASINS = REGIONS = {
    'Pacific': Region('Pacific', Location([1.111, 2.222]),
                      Location([-1.111, -2.222])),
    'East_Pacific': Region('East Pacific', Location([0, 0]), Location([1, 1]),
                           Location([3, 3]))
    # TODO define the rest of the basins...maybe define bounds for
    # other groupings
}
