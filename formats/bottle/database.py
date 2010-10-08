"""libcchdo.bottle.database

This writer expects the database to be using a schema like so:

cruises: expocode, etc...
casts: id, expocode, station, cast
cast_bottle_metadata: cast_id, latitude, longitude, depth
ctds: cast_id, latitude, longitude, depth, datetime, instr_id
bottles: id, cast_id, sample, bottle, datetime, flag_woce, flag_igoss
data_bottles: bottle_id, parameter_id, value, flag_woce, flag_igoss
data_ctds: ctd_id, parameter_id, value, flag_woce, flag_igoss
"""

from ...db import connect # cchdo_data()

#def read(self):
def write(self):
    print self.to_dict()
