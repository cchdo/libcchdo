from libcchdo.model.datafile import DataFile, SummaryFile, DataFileCollection


class NavCoord(object):
    def __init__(self, lon, lat, stnnbr=None, date=None, code=None):
        """Create a navigation coordinate used for outputting tracks.

        """
        self.lon = lon
        self.lat = lat
        self._stnnbr = stnnbr
        self._date = date
        self._code = code

    @property
    def stnnbr(self):
        if not self._stnnbr:
            return ''
        return self._stnnbr

    @property
    def date(self):
        return self.dt_to_nav_str(self._date)

    @property
    def code(self):
        """The position code."""
        if not self._code:
            return 'BO'
        return self._code

    @staticmethod
    def dt_to_nav_str(dt):
        """Return the string representation of a datetime.

        If no datetime is given, return an empty string.

        """
        try:
            return dt.strftime('%Y-%m-%d')
        except AttributeError:
            return ''

    def __str__(self):
        """Convert the NavCoord to a string representation."""
        return 'NavCoord({0}, {1}, {2}, {3})'.format(
            self.lon, self.lat, self.stnnbr, self.date, self.code)


class NavCoords(list):
    def __init__(self, navcoords):
        self.navcoords = navcoords


class TabbedNavCoords(NavCoords):
    def navcoord_str(self, coord):
        """Print TabbedNavCoord by joining with tabs.

        If none of stnnbr, date, or code exist, the resulting line will only
        have the coordinates.

        """
        items = [coord.lon, coord.lat, coord.stnnbr, coord.date, coord.code]
        if not (coord._stnnbr or coord._date or coord._code):
            items = items[:2]
        return '\t'.join(map(str, items))
        
    def __str__(self):
        return '\n'.join([self.navcoord_str(coord) for coord in self.navcoords])


class LinestringNavCoords(NavCoords):
    def navcoord_str(self, coord):
        """Print TabbedNavCoord by joining with tabs.

        If none of stnnbr, date, or code exist, the resulting line will only
        have the coordinates.

        """
        items = [coord.lon, coord.lat]
        return ' '.join(map(str, items))

    def __str__(self):
        """Convert to Linestring."""
        return 'LINESTRING({0})'.format(
            ','.join([self.navcoord_str(coord) for coord in self.navcoords]))


def iter_coords(self, navcoordcls, callback):
    """Iterate over the given object's sets of coordinates.

    There are three possibilities for self:
        1. DataFile
        2. SummaryFile
        3. DataFileCollection

    Arguments:
    callback - function(expocode, NavCoords)

    """
    expocode_coords = {}
    if (isinstance(self, DataFile) or isinstance(self, SummaryFile)):
        # TODO it is possible for a datafile to have a global expocode...
        expocodes = self.expocodes()

        lons = self['LONGITUDE'].values
        lats = self['LATITUDE'].values
        stnnbrs = self['STNNBR'].values
        dates = self['_DATETIME'].values
        try:
            codes = self['_CODE'].values
        except KeyError:
            codes = [None] * len(lons)

        for expocode in expocodes:
            elons = []
            elats = []
            estnnbrs = []
            edates = []
            ecodes = []

            for i, x in enumerate(self['EXPOCODE'].values):
                if x != expocode:
                    continue
                elons.append(lons[i])
                elats.append(lats[i])
                estnnbrs.append(stnnbrs[i])
                edates.append(dates[i])
                ecodes.append(codes[i])

            items = zip(elons, elats, estnnbrs, edates, ecodes)
            try:
                expocode_coords[expocode].extend(items)
            except KeyError:
                expocode_coords[expocode] = items
    elif isinstance(self, DataFileCollection):
        coords = [
            [file.globals['LONGITUDE'],
             file.globals['LATITUDE']] for file in self]
        try:
            expocode_coords[file.globals['EXPOCODE']].extend(coords)
        except KeyError:
            expocode_coords[file.globals['EXPOCODE']] = coords
    else:
        raise ArgumentError("Don't know how to write a nav file from that.")

    for expocode in sorted(expocode_coords.keys()):
        coords = expocode_coords[expocode]
        coords = [NavCoord(*coord) for coord in coords]
        callback(expocode, navcoordcls(coords))
