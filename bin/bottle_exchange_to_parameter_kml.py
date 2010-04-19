#!/usr/bin/env python

from __future__ import with_statement
from sys import argv, exit, path, stdout
path.insert(0, '/'.join(path[0].split('/')[:-1]))
from libcchdo import DataFile
from formats.bottle.exchange import exchange

if len(argv) < 2:
  print 'Usage:', argv[0], '<exbot file>'
  exit(1)
file = libcchdo.DataFile()
with open(argv[1], 'r') as in_file:
  exchange(file).read(in_file)

def color_arr_to_str(color):
  return 'ff'+''.join(map(lambda x: '%02x' % x, color[::-1]))

def map_to_color(val, min, max, mincolor, maxcolor):
  dratio = (val-min)/(max-min)
  dr = (maxcolor[0]-mincolor[0]) * dratio
  dg = (maxcolor[1]-mincolor[1]) * dratio
  db = (maxcolor[2]-mincolor[2]) * dratio
  return [mincolor[0]+dr, mincolor[1]+dg, mincolor[2]+db]

placemarks = []
maxtemp = 38
mintemp = 0
maxcolor = [255, 0, 0]
mincolor = [0, 0, 255]
for ctdtmp, lng, lat, depth, i in zip(
                              file.columns['CTDTMP'].values,
                              file.columns['LONGITUDE'].values,
                              file.columns['LATITUDE'].values,
                              file.columns['CTDPRS'].values,
                              range(0,len(file))):
  colorstr = color_arr_to_str(map_to_color(ctdtmp, mintemp, maxtemp, mincolor, maxcolor))
  placemarks.append("""
<Style id="dot%d">
  <IconStyle>
    <color>%s</color>
    <scale>0.5</scale>
    <Icon><href>http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png</href></Icon>
  </IconStyle>
</Style>
<Placemark>
  <styleUrl>#dot%d</styleUrl>
  <Point>
    <altitudeMode>relativeToGround</altitudeMode>
    <coordinates>%f,%f,-%d</coordinates>
  </Point>
</Placemark>""" % (i, colorstr, i, lng, lat, depth))

print """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
<name>Test</name>
%s
</Document></kml>""" % ''.join(placemarks)
