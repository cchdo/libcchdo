# libcchdo.bottle.database
# This writer expects the database to be using a schema like so:
#
# cruises: expocode, etc...
# casts: id, expocode, station, cast
# cast_bottle_metadata: cast_id, latitude, longitude, depth
# ctds: cast_id, latitude, longitude, depth, datetime, instr_id
# bottles: id, cast_id, sample, bottle, datetime, flag_woce, flag_igoss
# data_bottles: bottle_id, parameter_id, value, flag_woce, flag_igoss
# data_ctds: ctd_id, parameter_id, value, flag_woce, flag_igoss
#

from format import format

def connect_pg_cchdo_data():
  try:
    return pgdb.connect(user='libcchdo',
                        password='((hd0hydr0d@t@',
                        host='goship.ucsd.edu',
                        database='cchdo_data')
  except pgdb.Error, e:
    raise IOError("Database error: %s" % e)

class database(format):
  #def read(self):
  def write(self):
    print self.datafile.to_hash()
