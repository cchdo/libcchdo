''' libcchdo.common.track_line '''

from ..libcchdo import connect_mysql

from format import format

class track_line(format):
  #def read(self, handle):
  def write(self, handle):
    '''How to write a trackline entry to the MySQL database'''
    connection = connect_mysql()
    cursor = connection.cursor()
    expocodes = self.datafile.columns['EXPOCODE'].values
    for expocode in self.datafile.expocodes():
      indices = [i for i, x in enumerate(expocodes) if x == expocode]
      lngs = [self.datafile.columns['LONGITUDE'][i] for i in indices]
      lats = [self.datafile.columns['LATITUDE'][i] for i in indices]
      points = zip(lngs, lats)
      linestring = 'LINESTRING('+','.join(map(lambda p: ' '.join(p), points))+')'
      sql = ('SET @g = LineStringFromText("'+linestring+'");'
             'INSERT IGNORE INTO track_lines '
             'VALUES(DEFAULT,"'+expocode+'",@g,"Default")'
             'ON DUPLICATE KEY UPDATE Track = @g')
      cursor.execute(sql)
    cursor.close()
    connection.close()
