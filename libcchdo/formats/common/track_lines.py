from contextlib import closing

from libcchdo import LOG
from libcchdo.model.navcoord import iter_coords, LinestringNavCoords


#def read(self, handle):


def write(self, connection):
    """How to write a trackline entry to the cchdo database.

    This method currently assumes bottle formatted datafile.

    """
    def write_cruise_coords(expocode, coords):
        """Persist the given cruise coords pair.

        """
        sql = ('SET @g = LineStringFromText("{linestring}");\n'
               'INSERT IGNORE INTO track_lines '
               'VALUES(DEFAULT,"{expocode}",@g,"Default") '
               'ON DUPLICATE KEY UPDATE Track = @g').format(
            linestring=coords, expocode=expocode)
        with connection.begin() as trans:
            connection.execute(sql)
        LOG.info(u'Persisted track for {0}\n{1}'.format(expocode, coords))

    iter_coords(self, LinestringNavCoords, write_cruise_coords)
