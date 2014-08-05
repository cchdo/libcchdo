from collections import OrderedDict
from csv import reader as csv_reader

from libcchdo.db.util import wkt_to_track, tracks_for_cruises
from libcchdo.log import LOG
from libcchdo.plot.etopo import plot, plot_line_dots, plt, ETOPOBasemap
from libcchdo.plot import presets_goship, gmt_color

class LineRep(object):
    """WOCE line representation on this map.

    WOCE lines are represented by cruises. Sometimes it takes more than one
    cruise to accurately represent the line.

    Roxanne Lee painstakingly gathered this list.

    """
    def __init__(self, name, points, *cruise_expocodes):
        self.name = name
        self.cruises = cruise_expocodes
        if points is None:
            self.points = []
            for track, expo in tracks_for_cruises(*self.cruises):
                self.points.extend(track)
        else:
            self.points = points

    def __unicode__(self):
        return u'LineRep({0}, {1})'.format(self.name, self.cruises)

    def __repr__(self):
        return unicode(self)
    

def read_rep_lines(file):
    rows = csv_reader(file)
    for row in rows:
        yield row


def plot_woce_representation(args, file_path):
    """Take a CSV and plot specified cruises on a map.

    The CSV is formatted as::

    WOCE lines for map
    GenericLine,ExpoCode,Line,Year,SecondaryExpoCode,SecondaryLine,SecondaryYear

    WOCE lines for map
    GenericLine,ExpoCode,Line,Year,SecondaryExpoCode,SecondaryLine,SecondaryYear

    """
    woce_repr_sheet = (
        "https://docs.google.com/spreadsheet/"
        "ccc?key=0AseZhdC_bXrXdFdwbEVCZUpzYUpocUNUQnZVYk00TGc#gid=0")
    basins = OrderedDict()
    try:
        with open(file_path) as ifile:
            basin = None
            for row in read_rep_lines(ifile):
                if (
                        row[0] == 'WOCE lines for map' or
                        row[0] == '' or 
                        row[0] == 'Line'):
                    continue
                if row[1] == '':
                    basin = row[0]
                    continue

                expos = [x for x in [row[1], row[4]] if x != '']
                rep = LineRep(row[0], None, *expos)
                try:
                    basins[basin].append(rep)
                except KeyError:
                    basins[basin] = [rep]
    except (OSError, IOError), err:
        LOG.error(u'Cannot plot without a WOCE representation spreadsheet.')
        LOG.info(
            u'Download the spreadsheet as a CSV from {0} and put it at '
            '{1}'.format(woce_repr_sheet, file_path))
        return

    dot_size = 170
    if args.draft:
        dot_size = 3
    if args.large_dots:
        dot_size *= 2.35

    args, bm, gmt_style = presets_goship(
        gmt_color(0xFF, 0x88, 0x88),
        dot_size=dot_size, args=args, draft=args.draft)

    for basin in basins:
        for linerep in basins[basin]:
            xxx = []
            yyy = []
            for iii, jjj in linerep.points:
                xxx.append(iii)
                yyy.append(jjj)

            xxx, yyy = bm(xxx, yyy)

            if not xxx and not yyy:
                print 'unable to plot ', linerep
            else:
                dots = bm.scatter(xxx, yyy, **gmt_style)
    bm.savefig(args.output_filename)
