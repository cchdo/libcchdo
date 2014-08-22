from contextlib import closing

from libcchdo import LOG
from libcchdo.model.navcoord import iter_coords, LinestringNavCoords

from libcchdo.datadir.store import get_datastore


#def read(self, handle):


def write(self):
    """How to write a trackline entry to the cchdo database.

    This method currently assumes bottle formatted datafile.

    """
    def write_cruise_coords(expocode, coords):
        """Persist the given cruise coords pair.

        """
        dstore = get_datastore()
        dstore.write_cruise_coords(expocode, coords)
        LOG.info(u'Persisted track for {0}\n{1}'.format(expocode, coords))

    iter_coords(self, LinestringNavCoords, write_cruise_coords)
