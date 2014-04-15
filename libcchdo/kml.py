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


def get_attribute(obj, attribute, default=''):
    try:
        return __getattr__(cruise, attribute) or default
    except AttributeError:
        return default
    

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

    keymap = [
        ['line', 'Line'],
        ['country', 'Country'],
        # TODO chief scientist
        ['date_start', 'Begin_Date'],
        ['date_end', 'EndDate'],
        ['ship', 'Ship_Name'],
        ['alias', 'Alias'],
        ['group', 'Group'],
        ['program', 'Program'],
        ['link', 'link'],
    ]
    for key, attr in keymap:
        info[key] = get_attribute(cruise, attr)
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
    
    print KMLWriter.wrap("<name>Test</name>%s" % ''.join(placemarks))


def linestring_to_coords(linestring):
    """Convert an SQL LINESTRING into a list of tuple coordinates."""
    coordstr = string.translate(linestring[11:-1], string.maketrans(', ', ' ,'))
    return [tuple(x.split(',')) for x in coordstr.split(' ')]


class KMLWriter(object):
    @classmethod
    def header(cls):
        return """\
<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"
     xmlns:gx="http://www.google.com/kml/ext/2.2"
     xmlns:kml="http://www.opengis.net/kml/2.2"
     xmlns:atom="http://www.w3.org/2005/Atom">
  <Document>"""

    @classmethod
    def footer(cls):
        return "</Document></kml>"

    @classmethod
    def wrap(cls, string):
        return cls.header() + string + cls.footer()


class GenericCCHDOKML(KMLWriter):
    cycle_colors = map(
        _color_arr_to_str,
        [[255, 0, 0], [0, 255, 0], [0, 0, 255], [255, 255, 0]])

    @classmethod
    def styles(cls, starticon='flag'):
        xml = ""
        for i, color in enumerate(cls.cycle_colors):
            xml += """\
<Style id="linestyle">
  <LineStyle>
    <width>4</width>
    <color>{0}</color>
  </LineStyle>
</Style>""".format(color)
        if starticon == 'ship':
            starticon = "http://cchdo.ucsd.edu/images/map_search/cruise_start_icon.png"
        else:
            starticon = "http://maps.google.com/mapfiles/kml/shapes/flag.png"
        return xml + """\
<Style id="start">
  <IconStyle>
    <scale>1.5</scale>
    <Icon>
      <href>{starticon}</href>
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
</Style>""".format(starticon=starticon)

    @classmethod
    def _lsid(cls, iii):
        return iii % len(cls.cycle_colors)

    @classmethod
    def folder_for_cruise(cls, expocode, counter, coords, extra=''):
        points = []
        for coord in coords:
            points.append("<Point><coordinates>{0}</coordinates></Point>".format(
                ','.join(coord)))
        coordstr = ' '.join([','.join(ccc) for ccc in coords])
        return """\
<Folder>
  <name>{expo}</name>
  <Placemark>
    <styleUrl>#linestyle{lsid}</styleUrl>
    <LineString>
      <tessellate>1</tessellate>
      <coordinates>{coordstr}</coordinates>
    </LineString>
  </Placemark>
  <Placemark>
    <styleUrl>#start</styleUrl>
    <name>{expo}</name>
    <description>http://cchdo.ucsd.edu/cruise/{expo}</description>
    <Point><coordinates>{startcoord}</coordinates></Point>
  </Placemark>
  <Placemark>
  <styleUrl>#pt</styleUrl>
    <MultiGeometry>
      {multi}
    </MultiGeometry>
  </Placemark>
  {extra}
</Folder>
""".format(expo=expocode, lsid=cls._lsid(counter), coordstr=coordstr,
           startcoord=','.join(coords[0]), multi=''.join(points), extra=extra)


def db_to_kml(output, expocode=None, full=False):
    """Output kml of all CCHDO holdings

    """
    if expocode:
        kmldoc = "<name>CCHDO holdings for {0}</name>".format(expocode)
        kmldoc += GenericCCHDOKML.styles()
    else:
        kmldoc = "<name>CCHDO holdings</name>"
        kmldoc += GenericCCHDOKML.styles(starticon="ship")
    folders = []

    connection = connect.cchdo().connect()
    cursor = connection.connection.cursor()
    if full:
        sql = """SELECT track_lines.ExpoCode,
ASTEXT(track_lines.track),
cruises.Begin_Date,cruises.EndDate
FROM track_lines, cruises WHERE cruises.ExpoCode = track_lines.ExpoCode"""
    else:
        sql = 'SELECT ExpoCode,ASTEXT(track) FROM track_lines'
        if expocode:
            sql += ' WHERE ExpoCode = {0!r}'.format(expocode)
    cursor.execute(sql)
    rows = cursor.fetchall()
    counter = 2
    extra = ''
    for i, row in enumerate(rows):
        if full:
            expocode, coordstr, begin, end = row
            extra = '<TimeSpan><begin>%s</begin><end>%s</end></TimeSpan>' % (begin, end)
        else:
            expocode, coordstr = row
            counter = i
        coords = uniquify(linestring_to_coords(coordstr))
        folders.append(GenericCCHDOKML.folder_for_cruise(expocode, counter, coords, extra))
    cursor.close()
    connection.close()

    kmldoc += ''.join(folders)
    output.write(GenericCCHDOKML.wrap(kmldoc))


def nav_to_kml():
    from libcchdo.datadir.util import do_for_cruise_directories

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
            coords = [line.split() for line in f.readlines()]
        if not coords:
            return False

        kmldoc = GenericCCHDOKML.styles()
        kmldoc +=  GenericCCHDOKML.folder_for_cruise(expocode, generate_kml_from_nav.i, coords)

        with open(outputdir+'/track_'+expocode+'.kml', 'w') as f:
            f.write(GenericCCHDOKML.wrap(kmldoc))
        print 'Generated KML for %s.' % root
        generate_kml_from_nav.i += 1
    generate_kml_from_nav.i = 0

    def generate_kml_from_nav_into(dir):
        return lambda root, dirs, files: generate_kml_from_nav(
                                             root, dirs, files, dir)

    cwd=getcwd()
    directory = cwd + '/KML_CCHDO_holdings_' + \
        translate(str(datetime.utcnow()), maketrans(' :.', '___'))
    if not os.path.exists(directory):
        makedirs(directory)
    do_for_cruise_directories(generate_kml_from_nav_into(directory))
