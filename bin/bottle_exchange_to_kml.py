#!/usr/bin/env python

from __future__ import with_statement
import sys

import implib
import libcchdo.model.datafile
import libcchdo.formats.bottle.exchange as botex


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<exbot file>'
        return 1
    
    with open(argv[1], 'r') as in_file:
        file = libcchdo.model.datafile.DataFile()
        botex.read(file, in_file)
    
        placemarks = ['%f,%f' % coord for coord \
            in zip(file.columns['LONGITUDE'].values,
                   file.columns['LATITUDE'].values)]
    
        print """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"
     xmlns:gx="http://www.google.com/kml/ext/2.2"
     xmlns:kml="http://www.opengis.net/kml/2.2"
     xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
<name></name>
<Style id="linestyle">
  <LineStyle>
    <width>4</width>
    <color>ff0000ff</color>
  </LineStyle>
</Style>
<Placemark>
<styleUrl>#linestyle</styleUrl>
<LineString>
  <tessellate>1</tessellate>
  <coordinates>%s</coordinates>
</LineString>
</Placemark>
</Document></kml>""" % ' '.join(placemarks)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
