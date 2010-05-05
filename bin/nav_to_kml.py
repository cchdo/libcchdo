#!/usr/bin/env python

from __future__ import with_statement

from datetime import datetime
from string import translate, maketrans
from os import path, makedirs, getcwd
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-1]))

import datadir.util


def color_arr_to_str(color):
    return 'ff'+''.join(map(lambda x: '%02x' % x, color[::-1]))


kml_header = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"
     xmlns:gx="http://www.google.com/kml/ext/2.2"
     xmlns:kml="http://www.opengis.net/kml/2.2"
     xmlns:atom="http://www.w3.org/2005/Atom"><Document>"""
kml_footer = """</Document></kml>"""

cwd=getcwd()
directory = cwd+'/KML_CCHDO_holdings_'+translate(str(datetime.utcnow()),
                                                 maketrans(' :.', '___'))
if not path.exists(directory):
    makedirs(directory)

cycle_colors = map(color_arr_to_str, [[255, 0, 0], [0, 255, 0],
                                      [0, 0, 255], [255, 255, 0]])


def generate_kml_from_nav(root, dirs, files, outputdir):
    try:
        navfile = filter(lambda x: x.endswith('na.txt'), files)[0]
    except:
        navfile = None
    if not 'ExpoCode' in files or not navfile:
        print 'Skipping KML generation for %s. Not enough info found.' % root
        return False
    print 'Attempting generation for %s.' % root
    with open(path.join(root, 'ExpoCode'), 'r') as f:
        expocode = f.read()[:-1]
    # use nav file to gen kml
    with open(path.join(root, navfile), 'r') as f:
        coords = map(lambda l: l.split(), f.readlines())
    if not coords: return False
    placemarks = []
    placemarks.append("""
<Style id="linestyle">
  <LineStyle>
    <width>4</width>
    <color>%s</color>
  </LineStyle>
</Style>""" % cycle_colors[generate_kml_from_nav.i%len(cycle_colors)])
    placemarks.append("""
<Placemark>
  <name>%s</name>
  <styleUrl>#linestyle</styleUrl>
  <LineString>
    <tessellate>1</tessellate>
    <coordinates>%s</coordinates>
  </LineString>
</Placemark>""" % (expocode, ' '.join(map(lambda c: '%s,%s' % (c[0], c[1]),
                                          coords))))
    placemarks.append("""
<Placemark>
  <styleUrl>#start</styleUrl>
  <name>%s</name>
  <description>http://cchdo.ucsd.edu/data_access/show_cruise?ExpoCode=%s</description>
  <Point><coordinates>%s,%s</coordinates></Point>
</Placemark>""" % (expocode, expocode, coords[0][0], coords[0][1]))
    placemarks.append('<Folder>')
    for coord in coords:
        placemarks.append("""
<Placemark>
  <styleUrl>#pt</styleUrl>
  <description>%s,%s</description>
  <Point><coordinates>%s,%s</coordinates></Point>
</Placemark>""" % (coord[0], coord[1], coord[0], coord[1]))
    placemarks.append('</Folder>')

    with open(outputdir+'/track_'+expocode+'.kml', 'w') as f:
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
    print 'Generated KML for %s.' % root
    generate_kml_from_nav.i += 1

generate_kml_from_nav.i = 0


def generate_kml_from_nav_into(dir):
    return lambda root, dirs, files: generate_kml_from_nav(root, dirs,
                                                           files, dir)

datadir.util.do_for_cruise_directories(generate_kml_from_nav_into(directory))
