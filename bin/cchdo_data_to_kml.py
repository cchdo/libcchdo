#!/usr/bin/env python

from __future__ import with_statement
import datetime
import os
import string

import abs_import_libcchdo
import libcchdo.db.connect


def color_arr_to_str(color):
    return 'ff'+''.join(map(lambda x: '%02x' % x, color[::-1]))


kml_header = """\
<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"
     xmlns:gx="http://www.google.com/kml/ext/2.2"
     xmlns:kml="http://www.opengis.net/kml/2.2"
     xmlns:atom="http://www.w3.org/2005/Atom"><Document>"""
kml_footer = """</Document></kml>"""

directory = './KML_CCHDO_holdings_'+string.translate(
    str(datetime.datetime.utcnow()), string.maketrans(' :.', '___'))
if not os.path.exists(directory):
    os.makedirs(directory)

cycle_colors = map(color_arr_to_str, [[255, 0, 0], [0, 255, 0],
                                      [0, 0, 255], [255, 255, 0]])

connection = libcchdo.db.connect.cchdo()
cursor = connection.cursor()
cursor.execute('SELECT ExpoCode,ASTEXT(track) FROM track_lines')
rows = cursor.fetchall()
for i, row in enumerate(rows):
    expocode = row[0]
    placemarks = []
    coordstr = string.translate(row[1][11:], string.maketrans(', ', ' ,'))
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
