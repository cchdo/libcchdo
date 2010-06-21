#!/usr/bin/env python
# cchdo_data_to_kml

from __future__ import with_statement
import os.path
import string
from sys import argv, exit, path, stdout
path.insert(0, '/'.join(path[0].split('/')[:-1]))

import db.connect

def color_arr_to_str(color):
    return 'ff'+''.join(map(lambda x: '%02x' % x, color[::-1]))


kml_header = ('<?xml version="1.0" encoding="UTF-8"?>'
'<kml xmlns="http://www.opengis.net/kml/2.2" '
'xmlns:gx="http://www.google.com/kml/ext/2.2" '
'xmlns:kml="http://www.opengis.net/kml/2.2" '
'xmlns:atom="http://www.w3.org/2005/Atom"><Document>'
'<name>2010 CCHDO Holdings</name>'
'''<Style id="start">
  <IconStyle>
    <scale>1.5</scale>
    <Icon>'''
#'      <href>http://maps.google.com/mapfiles/kml/shapes/flag.png</href>'
'<href>http://cchdo.ucsd.edu/images/map_search/cruise_start_icon.png</href>'
'''    </Icon>
  </IconStyle>
</Style>
<Style id="linestyle">
  <LineStyle>
    <width>4</width>
    <color>ffff0000</color>
  </LineStyle>
</Style>
<Style id="pt">
  <IconStyle>
    <scale>0.7</scale>
    <color>ff0000ff</color>
    <Icon>
      <href>http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png</href>
    </Icon>
  </IconStyle>
</Style>'''
)
kml_footer = """</Document></kml>"""

directory = './KML_CCHDO_holdings'

connection = db.connect.cchdo()
cursor = connection.cursor()
cursor.execute((
    'SELECT track_lines.ExpoCode,'
    'ASTEXT(track_lines.track),'
    'cruises.Begin_Date,cruises.EndDate '
    'FROM track_lines, cruises WHERE cruises.ExpoCode = track_lines.ExpoCode'))
rows = cursor.fetchall()
with open(directory+'.kml', 'w') as f:
    f.write(kml_header)
    for i, row in enumerate(rows):
        expocode = row[0]
        placemarks = []
        coordstr = string.translate(row[1][11:], string.maketrans(', ', ' ,'))
        coords = map(lambda x: x.split(','), coordstr.split(' '))
        begin = row[2]
        end = row[3]
        placemarks.append('<Folder><name>%s</name>' % expocode)
        placemarks.append("""
<Placemark>
  <name>%s</name>
  <styleUrl>#linestyle</styleUrl>
  <LineString>
    <tessellate>1</tessellate>
    <coordinates>%s</coordinates>
  </LineString>
</Placemark>""" % (expocode, coordstr))
        placemarks.append((
'<Placemark><styleUrl>#start</styleUrl><name>%s</name>'
'<description>http://cchdo.ucsd.edu/data_access/show_cruise?ExpoCode=%s</description>'
'<Point><coordinates>%s,%s</coordinates></Point>'
'</Placemark>') % (expocode, expocode, coords[0][0], coords[0][1]))
        placemarks.append('<Folder>')
        for coord in coords:
            placemarks.append((
'<Placemark><styleUrl>#pt</styleUrl>'
'<Point><coordinates>%s,%s</coordinates></Point>'
'</Placemark>') % (coord[0], coord[1]))
        placemarks.append('</Folder>')
        placemarks.append('<TimeSpan><begin>%s</begin><end>%s</end></TimeSpan></Folder>' % (begin, end))
        f.write(''.join(placemarks))
    f.write(kml_footer)

cursor.close()
connection.close()