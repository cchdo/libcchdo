from datetime import datetime
import string
from string import translate, maketrans
from os import makedirs, getcwd
import os.path
import sys
from copy import copy

from lxml import etree

from pykml.factory import (
    KML_ElementMaker as KML,
    ATOM_ElementMaker as ATOM,
    GX_ElementMaker as GX,
    )

from libcchdo.db.model.legacy import session, Cruise
from libcchdo.fns import uniquify
from libcchdo.db import connect
from libcchdo.model.datafile import DataFile, SummaryFile, DataFileCollection


def _lon_lats(self):
    """Generates the longitude and latitude pairs for the given datafile.

    The data file may be a DataFile, SummaryFile, or DataFileCollection.

    """
    if (    isinstance(self, DataFile) or
            isinstance(self, SummaryFile)):
        for coord in zip(self['LONGITUDE'].values, self['LATITUDE'].values):
            yield tuple(coord)
    elif isinstance(self, DataFileCollection):
        for f in self.files:
            yield (f.globals['LONGITUDE'], f.globals['LATITUDE'])
    else:
        raise ArgumentError(
            u"Don't know how to get LATITUDE and LONGITUDE from {0}.".format(
                type(self)))


def coordinate(lon, lat, z=0.0):
    return ','.join(map(str, [lon, lat, z]))
    

def any_to_kml(self, output):
    expo = 'Unknown'
    if (    isinstance(self, DataFile) or
            isinstance(self, SummaryFile)):
        try:
            expo = self['EXPOCODE'].values[0]
        except (AttributeError, KeyError, IndexError):
            pass
    elif isinstance(self, DataFileCollection):
        try:
            expo = self.files[0].globals['EXPOCODE']
        except (AttributeError, KeyError):
            pass

    info = {}

    sesh = session()
    cruise = sesh.query(Cruise).filter(Cruise.ExpoCode == expo).first()
    try:
        info['line'] = cruise.Line or ''
    except AttributeError:
        info['line'] = ''
    try:
        info['country'] = cruise.Country or ''
    except AttributeError:
        info['country'] = ''
    try:
        info['chisci'] = cruise.Chief_Scientist or ''
    except AttributeError:
        info['chisci'] = ''
    try:
        info['date_start'] = cruise.Begin_Date or ''
    except AttributeError:
        info['date_start'] = ''
    try:
        info['date_end'] = cruise.EndDate or ''
    except AttributeError:
        info['date_end'] = ''
    try:
        info['ship'] = cruise.Ship_Name or ''
    except AttributeError:
        info['ship'] = ''
    try:
        info['alias'] = cruise.Alias or ''
    except AttributeError:
        info['alias'] = ''
    try:
        info['group'] = cruise.Group or ''
    except AttributeError:
        info['group'] = ''
    try:
        info['program'] = cruise.Program or ''
    except AttributeError:
        info['program'] = ''
    try:
        info['link'] = cruise.link or ''
    except AttributeError:
        info['link'] = ''
    sesh.close()

    infos = []
    for k, v in info.items():
        infos.append(KML.Data(
            KML.value(v),
            name=k,
        ))

    coords = uniquify(list(_lon_lats(self)))

    stations = []

    midlen = len(coords) / 2
    midcoord = [0, 0, 0]

    for i, coord in enumerate(coords):
        if i == midlen:
            midcoord = coord
            midpoint = KML.Point(
                KML.coordinates(coordinate(*coord))
            )
        else:
            stations.append(KML.Point(
                KML.coordinates(coordinate(*coord))
            ))

    balloon_text = """\
<html>
<head>
  <style>
    .logo { height: 3em; }
    .label { font-weight: bold; }
  </style>
</head>
<body>
  <div class="bln">
    <img class="logo" src="http://cchdo.ucsd.edu/images/logos/CCHDO_logo.png">
    <p>
    <span class="label"><a href="http://seahunt.ucsd.edu/cruise/$[expocode]">ExpoCode</a>:</span>
    <a href="http://cchdo.ucsd.edu/cruise/$[expocode]">$[expocode]</a>
    </p>
    <p>
    <span class="label">Line:</span>
    <span class="value">$[line]</span>
    </p>
    <p>
    <span class="label">Aliases:</span>
    <span class="value">$[alias]</span>
    </p>
    <p>
    <span class="label">Ship/Country:</span>
    <span class="value">$[ship]/$[country]</span>
    </p>
    <p>
    <span class="label">Chief Scientists:</span>
    <span class="value">$[chisci]</span>
    </p>
    <p>
    <span class="label">Cruise Dates:</span>
    <span class="value">$[date_start] - $[date_end]</span>
    </p>
  </div>
</body>
</html>
"""
    balloon_style = KML.BalloonStyle(
        KML.text(balloon_text),
        KML.textColor('ff000000'),
        KML.bgColor('ffebceb7'),
    )

    extended = KML.ExtendedData(
        KML.Data(
            KML.value(expo),
            name='expocode',
        ),
        *infos
    )

    kml = KML.Document(
        KML.name(expo),
        KML.LookAt(
            KML.longitude(midcoord[0]),
            KML.latitude(midcoord[1]),
            KML.altitude(0),
            KML.heading(0),
            KML.tilt(0),
            KML.range(7000.0 * (10 ** 3)),
        ),
        KML.Style(
            KML.IconStyle(
                KML.Icon(
                    KML.href('http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'),
                ),
                KML.color(_color_arr_to_str([64, 255, 96])),
                KML.scale('2.5'),
            ),
            copy(balloon_style),
            id='midpoint',
        ),
        KML.Style(
            KML.IconStyle(
                KML.Icon(
                    KML.href('http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'),
                ),
                KML.color(_color_arr_to_str([255, 128, 32])),
                KML.scale('1'),
            ),
            copy(balloon_style),
            id='stations',
        ),
        KML.Placemark(
            KML.styleUrl('#midpoint'),
            midpoint,
            KML.name(expo),
            copy(extended),
        ),
        KML.Placemark(
            KML.styleUrl('#stations'),
            KML.MultiGeometry(*stations),
            KML.name(),
            copy(extended),
        ),
    )
    
    output.write(etree.tostring(kml, pretty_print=True))


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
