import re
from datetime import datetime
from contextlib import closing

from libcchdo import LOG
from libcchdo.db.model import legacy
from libcchdo.db.model.legacy import TrackLine, Cruise

# This function is shamelessly stolen from geoalchemy which stole it from
# FeatureServer
def from_wkt (geom):
    """wkt helper: converts from WKT to a GeoJSON-like geometry."""
    wkt_linestring_match = re.compile(r'\(([^()]+)\)')
    re_space             = re.compile(r"\s+")

    coords = []
    for line in wkt_linestring_match.findall(geom):
        rings = [[]]
        for pair in line.split(","):

            if not pair.strip():
                rings.append([])
                continue
            rings[-1].append(map(float, re.split(re_space, pair.strip())))

        coords.append(rings[0])

    if geom.startswith("MULTIPOINT"):
        geomtype = "MultiPoint"
        coords = coords[0]
    elif geom.startswith("POINT"):
        geomtype = "Point"
        coords = coords[0][0]

    elif geom.startswith("MULTILINESTRING"):
        geomtype = "MultiLineString"
    elif geom.startswith("LINESTRING"):
        geomtype = "LineString"
        coords = coords[0]

    elif geom.startswith("MULTIPOLYGON"):
        geomtype = "MultiPolygon"
    elif geom.startswith("POLYGON"):
        geomtype = "Polygon"
    else:
        geomtype = geom[:geom.index("(")]
        raise Exception("Unsupported geometry type %s" % geomtype)

    return {"type": geomtype, "coordinates": coords}


def _grouped_cruises_with_data_modifications(
        lsesh, around_year=datetime(2008, 1, 1)):
    lDoc = legacy.Document
    query = lsesh.query(
        lDoc.ExpoCode, lDoc.LastModified,
        lDoc.Modified).\
        filter(lDoc.FileType != 'Directory').\
        filter(lDoc.FileType != 'Small Plot').\
        filter(lDoc.FileType != 'Large Plot').\
        filter(lDoc.FileType != 'Unrecognized').\
        filter(lDoc.FileType != 'Postscript file').\
        filter(lDoc.FileType != 'Directory Description').\
        filter(lDoc.FileType != 'Old Index HTML File').\
        filter(lDoc.FileType != 'Index HTML File').\
        filter(lDoc.FileType != 'Type HTML').\
        filter(lDoc.FileType != 'Person HTML').\
        filter(lDoc.FileType != 'Data History HTML').\
        filter(lDoc.FileType != 'Coordinates?').\
        filter(lDoc.FileType != None).\
        filter(lDoc.ExpoCode != 'NULL').\
        filter(lDoc.ExpoCode != None).\
        filter(lDoc.LastModified != None).\
        filter(lDoc.Modified != None).\
        order_by(lDoc.ExpoCode)
    documents = query.all()

    # Group all the modifications for each cruise first
    LOG.info(u'Grouping modification times for each cruise')
    cruise_modifications = {}
    for expocode, lastmod, modified in documents:
        if modified:
            modification_times = [
                datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                for x in modified.split(',')]
        else:
            modification_times = []

        try:
            cm = cruise_modifications[expocode]
            cm[0] = max(cm[0], lastmod)
            cm[1] = sorted(cm[1] + modification_times)
        except KeyError:
            cruise_modifications[expocode] = [lastmod, modification_times]

    # Now determine the add and modification time bin for each cruise
    pre = set()
    during = set()
    post = set()
    for expocode, (lastmod, modification_times) in cruise_modifications.items():
        try:
            date_added = modification_times[0]
        except IndexError:
            date_added = lastmod

        if not date_added or not lastmod:
            continue
        if not expocode or expocode == 'NULL':
            continue

        LOG.info(u'{0} {1} {2}'.format(expocode, lastmod, date_added))

        if date_added < around_year:
            if lastmod < around_year:
                pre.add(expocode)
            else:
                during.add(expocode)
        else:
            post.add(expocode)
    return [pre, during, post]


def wkt_to_track(track):
    """Convert WKT to a list of points."""
    return from_wkt(track)['coordinates']


def tracks_for_cruises(*expocodes):
    from libcchdo.datadir.store import get_datastore
    dstore = get_datastore()
    for xxx in dstore.tracks_for_cruises(*expocodes):
        yield xxx


def tracks(output, dt_from=None, dt_to=None, around=None):
    """Write track coordinates to a nav file from the legacy database.
    
    Arguments::
    dt_from --  (optional) datetime to limit to cruises after; can be used in
        conjunction with dt_to
    dt_to --  (optional) datetime to limit cruises before; can be used in
        conjunction with dt_from
    around -- (optional) datetime to bin around. Three bins will be created:
        created before last modified before, created before last modified after,
        created after last modified after.

    """
    def track_points(track, expocode, date_start):
        for coord in track:
            output.write(
                ','.join(map(str, [coord[0], coord[1], date_start])) + '\n')

    from libcchdo.datadir.store import get_datastore
    dstore = get_datastore()
    if around:
        around = int(around)
        def bin_end():
            LOG.info('bin end')
            output.write('\n')
        dstore.binned_tracks_callbacks(
            bin_end, track_points, around_year=datetime(around, 1, 1))
    elif dt_from:
        def bin_end():
            pass
        dstore.binned_tracks_callbacks(
            bin_end, track_points, dt_from=dt_from, dt_to=dt_to)
    else:
        def bin_end():
            pass
        dstore.binned_tracks_callbacks(bin_end, track_points)
