from datetime import datetime
from contextlib import closing

from geoalchemy.utils import from_wkt

from libcchdo import LOG
from libcchdo.db.model import legacy
from libcchdo.db.model.legacy import TrackLine, Cruise


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


def _tracks(bin_callback, track_callback,
            dt_from=None, dt_to=None, around_year=None):
    with closing(legacy.session()) as lsesh:
        if around_year is None:
            bins = [[]]
        else:
            bins = _grouped_cruises_with_data_modifications(
                lsesh, around_year=around_year)
        for range_bin in bins:
            query = lsesh.query(TrackLine.Track, Cruise.ExpoCode,
                                Cruise.Begin_Date).\
                join(Cruise, Cruise.ExpoCode == TrackLine.ExpoCode)
            if range_bin:
                query = query.filter(TrackLine.ExpoCode.in_(range_bin))

            if dt_from:
                query = query.filter(Cruise.Begin_Date >= dt_from)
            if dt_to:
                query = query.filter(Cruise.Begin_Date <= dt_to)
                
            tracks = query.all()
            for track, expocode, date_start in tracks:
                LOG.info(expocode)
                track = wkt_to_track(lsesh, track)
                track_callback(track, expocode, date_start)
            bin_callback()


def wkt_to_track(lsesh, track):
    """Convert WKT to a list of points."""
    return from_wkt(lsesh.scalar(track.wkt))['coordinates']


def tracks_for_cruises(*expocodes):
    with closing(legacy.session()) as lsesh:
        query = lsesh.query(TrackLine.Track, TrackLine.ExpoCode).\
            filter(TrackLine.ExpoCode.in_(expocodes))
        for track, expocode in query.all():
            track = wkt_to_track(lsesh, track)
            yield track, expocode


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

    if around:
        around = int(around)
        def bin_end():
            LOG.info('bin end')
            output.write('\n')
        _tracks(bin_end, track_points, around_year=datetime(around, 1, 1))
    elif dt_from:
        def bin_end():
            pass
        _tracks(bin_end, track_points, dt_from=dt_from, dt_to=dt_to)
    else:
        def bin_end():
            pass
        _tracks(bin_end, track_points)
