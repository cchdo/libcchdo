from datetime import datetime
from contextlib import closing

from geoalchemy.utils import from_wkt

from libcchdo import LOG
from libcchdo.db.model import legacy


def _grouped_cruises_with_data_modifications(lsesh,
                                             around_year=datetime(2008, 1, 1)):
    query = lsesh.query(
        legacy.Document.ExpoCode, legacy.Document.LastModified,
        legacy.Document.Modified).\
        filter(legacy.Document.FileType != 'Directory').\
        filter(legacy.Document.FileType != 'Small Plot').\
        filter(legacy.Document.FileType != 'Large Plot').\
        filter(legacy.Document.FileType != 'Unrecognized').\
        filter(legacy.Document.FileType != 'Postscript file').\
        filter(legacy.Document.FileType != 'Directory Description').\
        filter(legacy.Document.FileType != 'Old Index HTML File').\
        filter(legacy.Document.FileType != 'Index HTML File').\
        filter(legacy.Document.FileType != 'Type HTML').\
        filter(legacy.Document.FileType != 'Person HTML').\
        filter(legacy.Document.FileType != 'Data History HTML').\
        filter(legacy.Document.FileType != 'Coordinates?').\
        filter(legacy.Document.FileType != None).\
        filter(legacy.Document.ExpoCode != 'NULL').\
        filter(legacy.Document.ExpoCode != None).\
        filter(legacy.Document.LastModified != None).\
        filter(legacy.Document.Modified != None).\
        order_by(legacy.Document.ExpoCode)
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


def _tracks(bin_callback, track_callback, around_year=None):
    with closing(legacy.session()) as lsesh:
        if around_year is None:
            bins = [[]]
        else:
            bins = _grouped_cruises_with_data_modifications(
                lsesh, around_year=around_year)
        for range_bin in bins:
            query = lsesh.query(legacy.TrackLine.Track, legacy.Cruise.ExpoCode,
                                legacy.Cruise.Begin_Date).\
                join(legacy.Cruise,
                    legacy.Cruise.ExpoCode == legacy.TrackLine.ExpoCode)
            if range_bin:
                query = query.filter(legacy.TrackLine.ExpoCode.in_(range_bin))
                
            tracks = query.all()
            for track, expocode, date_start in tracks:
                LOG.info(expocode)
                track = from_wkt(lsesh.scalar(track.wkt))['coordinates']
                track_callback(track, expocode, date_start)
            bin_callback()


def tracks(output, around=None):
    def track_points(track, expocode, date_start):
        for coord in track:
            output.write(
                ','.join(
                    map(str, [coord[0], coord[1], date_start])) + '\n')

    if around:
        around = int(around)
        def bin_end():
            LOG.info('bin end')
            output.write('\n')

        _tracks(bin_end, track_points, datetime(around, 1, 1))
    else:
        def bin_end():
            pass
        _tracks(bin_end, track_points)
