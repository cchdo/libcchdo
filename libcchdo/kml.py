from datetime import datetime
import string
from string import translate, maketrans
from os import makedirs, getcwd
import os.path
import sys

from libcchdo.db import connect


def bottle_exchange_to_kml(self, output):
    placemarks = ['%f,%f' % coord for coord \
        in zip(self.columns['LONGITUDE'].values,
               self.columns['LATITUDE'].values)]

    output.write("""\
<?xml version="1.0" encoding="UTF-8"?>
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
</Document></kml>""" % ' '.join(placemarks))


def _color_arr_to_str(color):
    return 'ff'+''.join(map(lambda x: '%02x' % x, color[::-1]))


def _map_to_color(val, min, max, mincolor, maxcolor):
    dratio = (val - min) / (max - min)
    dr = (maxcolor[0] - mincolor[0]) * dratio
    dg = (maxcolor[1] - mincolor[1]) * dratio
    db = (maxcolor[2] - mincolor[2]) * dratio
    return [mincolor[0] + dr, mincolor[1] + dg, mincolor[2] + db]


def bottle_exchange_to_parameter_kml(self, output):
    placemarks = []
    maxtemp = 38
    mintemp = 0
    maxcolor = [255, 0, 0]
    mincolor = [0, 0, 255]
    for ctdtmp, lng, lat, depth, i in zip(
        self.columns['CTDTMP'].values, self.columns['LONGITUDE'].values,
        self.columns['LATITUDE'].values, self.columns['CTDPRS'].values,
        range(0, len(self))):
      colorstr = _color_arr_to_str(
          _map_to_color(ctdtmp, mintemp, maxtemp, mincolor, maxcolor))
      placemarks.append("""\
<Style id="dot%d">
  <IconStyle>
    <color>%s</color>
    <scale>0.5</scale>
    <Icon>
      <href>http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png</href>
    </Icon>
  </IconStyle>
</Style>
<Placemark>
  <styleUrl>#dot%d</styleUrl>
  <Point>
    <altitudeMode>relativeToGround</altitudeMode>
    <coordinates>%f,%f,-%d</coordinates>
  </Point>
</Placemark>""" % (i, colorstr, i, lng, lat, depth))
    
    print """\
<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"
     xmlns:gx="http://www.google.com/kml/ext/2.2"
     xmlns:kml="http://www.opengis.net/kml/2.2"
     xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
<name>Test</name>
%s
</Document></kml>""" % ''.join(placemarks)


def db_to_kml(self, output):
    kml_header = """\
    <?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2"
         xmlns:gx="http://www.google.com/kml/ext/2.2"
         xmlns:kml="http://www.opengis.net/kml/2.2"
         xmlns:atom="http://www.w3.org/2005/Atom"><Document>"""
    kml_footer = """</Document></kml>"""

    directory = './KML_CCHDO_holdings_'+string.translate(
        str(datetime.utcnow()), string.maketrans(' :.', '___'))
    if not os.path.exists(directory):
        makedirs(directory)

    cycle_colors = map(
        _color_arr_to_str,
        [[255, 0, 0], [0, 255, 0], [0, 0, 255], [255, 255, 0]])

    connection = connect.cchdo()
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


def db_to_kml_full(self, output):
    """
    With dates 2010-04-26.

    """
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

    connection = connect.cchdo()
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


def db_track_lines_to_kml():
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
        makedirs(directory)

    cycle_colors = map(
        _color_arr_to_str,
        [[255, 0, 0], [0, 255, 0], [0, 0, 255], [255, 255, 0]])

    connection = connect.cchdo()
    cursor = connection.cursor()
    cursor.execute('SELECT ExpoCode,ASTEXT(track) FROM track_lines')
    rows = cursor.fetchall()
    for i, row in enumerate(rows):
        expocode = row[0]
        placemarks = []
        coordstr = string.translate(row[1][11:], string.maketrans(', ', ' ,'))
        coords = map(lambda x: x.split(','), coordstr.split(' '))
        placemarks.append("""\
<Style id="linestyle">
  <LineStyle>
    <width>4</width>
    <color>%s</color>
  </LineStyle>
</Style>""" % cycle_colors[i%len(cycle_colors)])
    placemarks.append("""\
<Placemark>
  <name>%s</name>
  <styleUrl>#linestyle</styleUrl>
  <LineString>
    <tessellate>1</tessellate>
    <coordinates>%s</coordinates>
  </LineString>
</Placemark>""" % (expocode, coordstr))
    placemarks.append("""\
<Placemark>
  <styleUrl>#start</styleUrl>
  <name>%s</name>
  <description>http://cchdo.ucsd.edu/data_access/show_cruise?ExpoCode=%s</description>
  <Point><coordinates>%s,%s</coordinates></Point>
</Placemark>""" % (expocode, expocode, coords[0][0], coords[0][1]))
    for coord in coords:
        placemarks.append("""\
<Placemark>
  <styleUrl>#pt</styleUrl>
  <Point><coordinates>%s,%s</coordinates></Point>
</Placemark>""" % (coord[0], coord[1]))

    with open(directory+'/track_'+expocode+'.kml', 'w') as f:
        f.write("""\
%s<name>%s</name>
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


def nav_to_kml():
    from libcchdo.datadir.util import do_for_cruise_directories

    kml_header = """\
<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"
     xmlns:gx="http://www.google.com/kml/ext/2.2"
     xmlns:kml="http://www.opengis.net/kml/2.2"
     xmlns:atom="http://www.w3.org/2005/Atom"><Document>"""
    kml_footer = """</Document></kml>"""

    cwd=getcwd()
    directory = cwd + '/KML_CCHDO_holdings_' + \
        translate(str(datetime.utcnow()), maketrans(' :.', '___'))
    if not os.path.exists(directory):
        makedirs(directory)

    cycle_colors = map(
        _color_arr_to_str,
        [[255, 0, 0], [0, 255, 0], [0, 0, 255], [255, 255, 0]])

    def generate_kml_from_nav(root, dirs, files, outputdir):
        try:
            navfile = filter(lambda x: x.endswith('na.txt'), files)[0]
        except:
            navfile = None
        if not 'ExpoCode' in files or not navfile:
            print 'Skipping KML generation for %s. Not enough info found.' % root
            return False
        print 'Attempting generation for %s.' % root
        with open(os.path.join(root, 'ExpoCode'), 'r') as f:
            expocode = f.read()[:-1]
        # use nav file to gen kml
        with open(os.path.join(root, navfile), 'r') as f:
            coords = map(lambda l: l.split(), f.readlines())
        if not coords: return False
        placemarks = []
        placemarks.append("""\
<Style id="linestyle">
  <LineStyle>
    <width>4</width>
    <color>%s</color>
  </LineStyle>
</Style>""" % cycle_colors[generate_kml_from_nav.i%len(cycle_colors)])
        placemarks.append("""\
<Placemark>
  <name>%s</name>
  <styleUrl>#linestyle</styleUrl>
  <LineString>
    <tessellate>1</tessellate>
    <coordinates>%s</coordinates>
  </LineString>
</Placemark>""" % (expocode, ' '.join(map(lambda c: ','.join(c[:2]), coords))))
        placemarks.append("""\
<Placemark>
  <styleUrl>#start</styleUrl>
  <name>%s</name>
  <description>http://cchdo.ucsd.edu/data_access/show_cruise?ExpoCode=%s</description>
  <Point><coordinates>%s,%s</coordinates></Point>
</Placemark>""" % (expocode, expocode, coords[0][0], coords[0][1]))
        placemarks.append('<Folder>')
        for coord in coords:
            placemarks.append("""\
<Placemark>
  <styleUrl>#pt</styleUrl>
  <description>%s,%s</description>
  <Point><coordinates>%s,%s</coordinates></Point>
</Placemark>""" % (coord[0], coord[1], coord[0], coord[1]))
        placemarks.append('</Folder>')

        with open(outputdir+'/track_'+expocode+'.kml', 'w') as f:
            f.write("""\
%s<name>%s</name>
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
        return lambda root, dirs, files: generate_kml_from_nav(
                                             root, dirs, files, dir)

    do_for_cruise_directories(generate_kml_from_nav_into(directory))
