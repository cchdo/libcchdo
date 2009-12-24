#!/usr/bin/env python
# db_track_lines_to_kml

from __future__ import with_statement
try:
  import MySQLdb
except ImportError, e:
  print e, "\n", 'You should get MySQLdb from http://sourceforge.net/projects/mysql-python. You will need MySQL with server binaries installed already.'
  exit(1)
from datetime import datetime
from os import path, makedirs
from sys import exit
from string import translate, maketrans

def connect_mysql():
  try:
    return MySQLdb.connect(user='cchdo_server',
                           passwd='((hd0hydr0d@t@',
                           host='cchdo.ucsd.edu',
                           db='cchdo')
  except MySQLdb.Error, e:
    print "Database error: %s" % e
    exit(1)

def color_arr_to_str(color):
  return 'ff'+''.join(map(lambda x: '%02x' % x, color[::-1]))

kml_header = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom"><Document>"""
kml_footer = """</Document></kml>"""

directory = './KML_CCHDO_holdings_'+translate(str(datetime.utcnow()), maketrans(' :.', '___'))
if not path.exists(directory):
  makedirs(directory)

cycle_colors = map(color_arr_to_str, [[255, 0, 0], [0, 255, 0], [0, 0, 255], [255, 255, 0]])

connection = connect_mysql()
cursor = connection.cursor()
cursor.execute('SELECT ExpoCode,ASTEXT(track) FROM track_lines')
rows = cursor.fetchall()
for i, row in enumerate(rows):
  expocode = row[0]
  placemarks = []
  coordstr = translate(row[1][11:], maketrans(', ', ' ,'))
  coords = map(lambda x: x.split(','), coordstr.split(' '))
  placemarks.append("""
<Style id="linestyle">
  <LineStyle>
    <width>4</width>
    <color>%s</color>
  </LineStyle>
</Style>""" % cycle_colors[i%len(cycle_colors)])
  placemarks.append("""
<Placemark>
  <name>%s</name>
  <styleUrl>#linestyle</styleUrl>
  <LineString>
    <tessellate>1</tessellate>
    <coordinates>%s</coordinates>
  </LineString>
</Placemark>""" % (expocode, coordstr))
  placemarks.append("""
<Placemark>
  <styleUrl>#start</styleUrl>
  <name>%s</name>
  <description>http://cchdo.ucsd.edu/data_access/show_cruise?ExpoCode=%s</description>
  <Point><coordinates>%s,%s</coordinates></Point>
</Placemark>""" % (expocode, expocode, coords[0][0], coords[0][1]))
  for coord in coords:
    placemarks.append("""
<Placemark>
  <styleUrl>#pt</styleUrl>
  <Point><coordinates>%s,%s</coordinates></Point>
</Placemark>""" % (coord[0], coord[1]))

  with open(directory+'/track_'+expocode+'.kml', 'w') as f:
    f.write("""%s<name>%s</name>
<Style id="start">
  <IconStyle>
    <scale>1.5</scale>
    <Icon>
      <href>http://maps.google.com/mapfiles/kml/shapes/flag.png</href>
    </Icon>
  </IconStyle>
</Style>
<Style id="pt">
  <IconStyle>
    <scale>0.7</scale>
    <color>ff0000ff</color>
    <Icon>
      <href>http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png</href>
    </Icon>
  </IconStyle>
</Style>
%s%s""" % (kml_header, expocode, ''.join(placemarks), kml_footer))

cursor.close()
connection.close()