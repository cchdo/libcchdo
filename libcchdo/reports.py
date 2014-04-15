import re
from datetime import datetime, timedelta
from contextlib import closing
from zipfile import ZipFile
import os.path

from sqlalchemy.sql import not_, between
from sqlalchemy import distinct

from libcchdo.log import LOG, DEBUG, ERROR
from libcchdo.db.model.legacy import (
    session as lsession, Document, Cruise, Submission, QueueFile,
    )
from libcchdo.fns import InvalidOperation
from libcchdo.config import get_datadir_hostname, get_datadir_root
from libcchdo.datadir.dl import AFTP, SFTP
from libcchdo.model.datafile import DataFileCollection, SummaryFile
from libcchdo.formats.summary import woce as wocesum
from libcchdo.formats.ctd.zip import exchange as ctdzipex


types_to_ignore = [
    'Coord info', 'GMT info File', 'Large Plot', 'Postscript file',
    'Small Plot', 'Unrecognized', 'Directory', 'Coordinates?',
]


class DateRange(object):
    def __init__(self, dstart, dend):
        self.dstart = dstart
        self.dend = dend

    def __contains__(self, dt):
        return self.dstart < dt and dt < self.dend


def report_data_updates(args):
    """Counts updates within the time frame.

    Provide a summary of:
    * number of modifications to each file type
    * number of cruises with updated files

    """
    with closing(lsession()) as session:
        date_end = args.date_end
        date_start = args.date_start
        args.output.write('/'.join(map(str, [date_start, date_end])) + '\n')

        docs = session.query(Document).\
            filter(
                between(
                    Document.LastModified, date_start, date_end)).\
            filter(not_(Document.FileType.in_(types_to_ignore))).\
            all()

        # count modifications of file types
        type_edit_cruises = {}
        type_add_cruises = {}
        cruises = set()
        drange = DateRange(date_start, date_end)
        for doc in docs:
            if 'original' in doc.FileName or 'Queue' in doc.FileName:
                continue
            details = [doc.LastModified, doc.ExpoCode, doc.FileName]
            LOG.info(' '.join(map(str, details)))

            if not doc.Modified or len(doc.Modified.split(',')) == 1:
                try:
                    type_add_cruises[doc.FileType].append(doc.ExpoCode)
                except KeyError:
                    type_add_cruises[doc.FileType] = [doc.ExpoCode]
            else:
                for mtime in doc.Modified.split(','):
                    mtime = datetime.strptime(mtime, '%Y-%m-%d %H:%M:%S')
                    if mtime in drange:
                        LOG.info('\t{0}\n'.format(mtime))
                        cruises.add(doc.ExpoCode)
                        try:
                            type_edit_cruises[doc.FileType].append(doc.ExpoCode)
                        except KeyError:
                            type_edit_cruises[doc.FileType] = [doc.ExpoCode]
                    else:
                        pass
                        LOG.info('\t{0} out of range\n'.format(mtime))
        args.output.write(
            'Data updates from {0}/{1}:\n'.format(date_start, date_end))
        args.output.write(
            '# cruises supported: {0}\n'.format(session.query(distinct(Cruise.ExpoCode)).count()))
        args.output.write(
            '# cruises with data: {0}\n'.format(session.query(distinct(Document.ExpoCode)).count()))
        args.output.write(
            '# cruises with updated files: {0}\n'.format(len(cruises)))

        type_add_counts = {}
        for ftype, cruises in type_add_cruises.items():
            type_add_counts[ftype] = len(cruises)

        args.output.write(
            '# files added: {0}\n'.format(sum(type_add_counts.values())))

        type_edit_counts = {}
        for ftype, cruises in type_edit_cruises.items():
            type_edit_counts[ftype] = len(cruises)

        args.output.write(
            '# file updates: {0}\n'.format(sum(type_edit_counts.values())))
        args.output.write('File type add counts:\n')
        args.output.write(repr(type_add_counts) + '\n')
        args.output.write('File type edit counts:\n')
        args.output.write(repr(type_edit_counts) + '\n')
        args.output.write('Cruises with updated files:\n')
        args.output.write(repr(sorted(list(cruises))) + '\n')

        cruise_type_adds = {}
        for ftype, cruises in type_add_cruises.items():
            for cruise in cruises:
                try:
                    cruise_type_adds[cruise].add(ftype)
                except KeyError:
                    cruise_type_adds[cruise] = set([ftype])
        cruise_type_updates = {}
        for ftype, cruises in type_edit_cruises.items():
            for cruise in cruises:
                try:
                    cruise_type_updates[cruise].add(ftype)
                except KeyError:
                    cruise_type_updates[cruise] = set([ftype])

        args.output.write('Cruise file adds:\n')
        for cruise, types in cruise_type_adds.items():
            args.output.write('{0},{1}\n'.format(cruise, types))
        args.output.write('Cruise file updates:\n')
        for cruise, types in cruise_type_updates.items():
            args.output.write('{0},{1}\n'.format(cruise, types))

        args.output.write('Cruises with CTD adds:\n')
        for cruise, types in cruise_type_adds.items():
            found = False
            for ttt in types:
                if 'CTD' in ttt:
                    found = True
            if not found:
                continue
            args.output.write('{0}\n'.format(cruise))
        args.output.write('Cruises with CTD updates:\n')
        for cruise, types in cruise_type_updates.items():
            found = False
            for ttt in types:
                if 'CTD' in ttt:
                    found = True
            if not found:
                continue
            args.output.write('{0}\n'.format(cruise))


def report_submission_and_queue(args):
    """Counts submissions and queue updates.

    Provide a summary of:
    * number of submissions
    * number of queue updates

    """
    with closing(lsession()) as session:
        date_end = args.date_end
        date_start = args.date_start
        args.output.write('/'.join(map(str, [date_start, date_end])) + '\n')

        submissions_query = session.query(Submission).\
            filter(
                between(
                    Submission.submission_date, date_start, date_end)).\
            filter(Submission.email != 'tooz@oceanatlas.com')
        submissions = submissions_query.count()

        submissions_assimilated = session.query(Submission).\
            filter(
                between(
                    Submission.submission_date, date_start, date_end)).\
            filter(Submission.email != 'tooz@oceanatlas.com').\
            filter(Submission.assimilated == True).\
            count()

        submission_cruises = session.query(distinct(QueueFile.ExpoCode)).filter(
            QueueFile.submission_id.in_([x.id for x in submissions_query.all()])).all()

        queued = session.query(QueueFile).\
            filter(
                between(
                    QueueFile.date_received, date_start, date_end)).\
            filter(QueueFile.merged != 2).\
            count()

        queued_and_merged = session.query(QueueFile).\
            filter(
                between(
                    QueueFile.date_received, date_start, date_end)).\
            filter(QueueFile.merged == 1).\
            count()

        args.output.write(
            'Submissions from {0}/{1}:\n'.format(date_start, date_end))
        args.output.write(
            '# submissions: {0}\n'.format(submissions))
        args.output.write(
            '# submissions assimilated: {0}\n'.format(submissions_assimilated))
        args.output.write(
            '# cruises with submitted files: {0}\n'.format(len(submission_cruises)))
        args.output.write(
            '# queued: {0}\n'.format(queued))
        args.output.write(
            '# queued and merged: {0}\n'.format(queued_and_merged))
        args.output.write(
            '# queued and not merged: {0}\n'.format(queued - queued_and_merged))


def report_old_style_expocodes(args):
    """Counts expocodes that are not new-style.

    New style ExpoCode specification:
    http://cchdo.ucsd.edu/policies/postwoce_name.html

    """
    with closing(lsession()) as session:
        cruises = session.query(Cruise.ExpoCode, Cruise.Begin_Date).all()

        re_part_ship = '[A-Z0-9]{4}'
        re_part_date = '[0-9]{8}'
        re_new_style = re.compile(re_part_ship + re_part_date)

        expos_new_style = []
        expos_old_style = {
            'prewoce': [], 'woce': [], 'postwoce': [], 'unknown': []}
        expos_new_style_bad_date = []

        for c in cruises:
            expo = c.ExpoCode
            print >> args.output, expo, c.Begin_Date
            if re_new_style.match(expo):
                try:
                    datetime.strptime(expo[4:], '%Y%m%d')
                    expos_new_style.append(expo)
                except ValueError:
                    expos_new_style_bad_date.append(expo)
            else:
                date = c.Begin_Date
                if not date:
                    bin_name = 'unknown'
                else:
                    year = date.year
                    if year < 1990:
                        bin_name = 'prewoce'
                    elif year < 2000:
                        bin_name = 'woce'
                    else:
                        bin_name = 'postwoce'

                oldbin = expos_old_style[bin_name]
                oldbin.append(expo)

        print >> args.output, 'Old-style ExpoCodes'
        print >> args.output, 'prewoce:\t', expos_old_style['prewoce']
        print >> args.output, 'woce:\t\t', expos_old_style['woce']
        print >> args.output, 'postwoce:\t', expos_old_style['postwoce']
        print >> args.output, 'unknown:\t', expos_old_style['unknown']

        print >> args.output, 'New-style ExpoCodes'
        print >> args.output, 'good:\t', expos_new_style
        print >> args.output, 'bad date:\t', expos_new_style_bad_date

        print >> args.output
        print >> args.output, 'Counts'

        print >> args.output, 'Old-style ExpoCodes'
        print >> args.output, 'prewoce:\t', len(expos_old_style['prewoce'])
        print >> args.output, 'woce:\t\t', len(expos_old_style['woce'])
        print >> args.output, 'postwoce:\t', len(expos_old_style['postwoce'])
        print >> args.output, 'unknown:\t', len(expos_old_style['unknown'])
        print >> args.output, 'TOTAL:\t\t', \
            len(reduce(lambda x, y: x + y, expos_old_style.values(), []))

        print >> args.output, 'New-style ExpoCodes'
        print >> args.output, 'good:\t\t', len(expos_new_style)
        print >> args.output, 'bad date:\t', len(expos_new_style_bad_date)


def _pick_precedent_ctd_format(formats):
    if 'exchange' in formats:
        return 'exchange'
    if 'netcdf' in formats:
        return 'netcdf'
    if 'woce' in formats:
        if 'wocesum' in formats:
            return 'wocesum'
    if 'wocesum' in formats:
        return 'wocesum'
    raise ValueError(
        u'Could not pick most precedent CTD format from {0!r}'.format(formats))


class ArgoIndexProfile(object):
    def __init__(self, fname, date, lat, lon, ocean, profiler_type, inst, mtime):
        self.fname = fname
        self.date = date
        self.lat = lat
        self.lon = lon
        self.ocean = ocean
        self.profiler_type = profiler_type
        self.inst = inst
        self.mtime = mtime


    def __unicode__(self):
        return ','.join([unicode(x) for x in [
            self.fname, self.date, self.lat, self.lon, self.ocean,
            self.profiler_type, self.inst, self.mtime]])
        

class ArgoIndexFile(object):
    title = 'Profile directory file of the CLIVAR and Carbon Hydrographic Data Office'
    description = 'The directory file describes all individual profile files of the CCHDO.'
    project = 'CCHDO'
    format_version = '2.0'

    profiles = []

    def header(self):
        headers = [
            'Title', 'Description', 'Project', 'Format version',
            'Date of update']
        header_comments = [
            '# {0} : {{{1}}}'.format(x, i) for i, x in enumerate(headers)]
        formatstr = '\n'.join(header_comments)
        return formatstr.format(
            self.title, self.description, self.project, self.format_version,
            datetime.utcnow().strftime('%Y%m%d%H%M%S'))

    def column_header(self):
        return ArgoIndexProfile(
            'file', 'date', 'latitude', 'longitude', 'ocean', 'profiler_type',
            'institution', 'date_update')

    def append(self, profile):
        self.profiles.append(profile)

    def __unicode__(self):
        profiles = [self.header(), self.column_header()] + self.profiles
        return '\n'.join([unicode(x) for x in profiles])

    def __str__(self):
        return unicode(self)


def report_argo_ctd_index(args):
    """Generates an Argo style index file of all CTD profiles.

    http://www.usgodae.org/pub/outgoing/argo/ar_index_global_prof.txt
    file,date,latitude,longitude,ocean,profiler_type,institution,date_update
    aoml/13857/profiles/R13857_001.nc,19970729200300,0.267,-16.032,A,845,AO,20080918131927

    """
    directories = []
    with closing(lsession()) as session:
        dirs = session.query(Document).filter(Document.FileType == 'Directory').all()
        for directory in dirs:
            if 'Queue' in directory.FileName:
                continue
            if 'ExpoCode' not in directory.Files:
                continue
            directories.append(directory)

    sftp = SFTP()
    sftp.connect(get_datadir_hostname())
    aftp = AFTP(sftp)

    argo_index = ArgoIndexFile()
    for directory in directories:
        ctd_files = {}
        files = directory.Files.split('\n')
        for fname in files:
            if fname.endswith('ct1.zip'):
                ctd_files['exchange'] = fname
            elif fname.endswith('nc_ctd.zip'):
                ctd_files['netcdf'] = fname
            elif fname.endswith('ct.zip'):
                ctd_files['woce'] = fname
            elif fname.endswith('su.txt'):
                ctd_files['wocesum'] = fname

        if not ctd_files:
            continue
        
        try:
            precedent_format = _pick_precedent_ctd_format(ctd_files.keys())
        except ValueError:
            continue

        cruise_dir = directory.FileName
        ctd_file = ctd_files[precedent_format]
        path = os.path.join(cruise_dir, ctd_file)

        LOG.debug(path)
        try:
            mtime = aftp.mtime(path)
            mtime = mtime.strftime('%Y%m%d%H%M%S')
        except IOError:
            LOG.error(u'Could not open file {0}'.format(path))
        if precedent_format == 'exchange':
            files = DataFileCollection()
            with aftp.dl(path) as fff:
                if fff is None:
                    LOG.error(u'Could not find file {0}'.format(path))
                    continue
                LOG.setLevel(ERROR)
                try:
                    ctdzipex.read(files, fff, header_only=True)
                except (ValueError, InvalidOperation):
                    LOG.error(u'Unable to read {0}'.format(path))
                LOG.setLevel(DEBUG)

            for ctdfile in files:
                fpath = path + '#' + ctdfile.globals['_FILENAME']
                date = ctdfile.globals['_DATETIME']
                if date is None:
                    date = ''
                else:
                    date = date.strftime('%Y%m%d%H%M%S')
                lat = ctdfile.globals['LATITUDE']
                lon = ctdfile.globals['LONGITUDE']
                ocean = ''
                profiler_type = ''
                inst = ''
                argo_index.append(ArgoIndexProfile(
                    fpath, date, lat, lon, ocean, profiler_type, inst, mtime
                ))
        elif precedent_format == 'netcdf':
            # TODO currently there aren't any files that have netcdf precedent
            args.output.write('netcdf!!!' + path + '\n')
        elif precedent_format == 'wocesum':
            sumfile = SummaryFile()
            path = os.path.join(get_datadir_root(), path)
            with aftp.dl(path) as fff:
                if fff is None:
                    LOG.error(u'Could not find file {0}'.format(path))
                    continue
                LOG.setLevel(ERROR)
                wocesum.read(sumfile, fff)
                LOG.setLevel(DEBUG)

            for iii in range(len(sumfile)):
                fpath = path + '#' + str(iii)
                date = sumfile['_DATETIME'][iii]
                if date is None:
                    date = ''
                else:
                    date = date.strftime('%Y%m%d%H%M%S')
                lat = sumfile['LATITUDE'][iii]
                lon = sumfile['LONGITUDE'][iii]
                ocean = ''
                profiler_type = ''
                inst = ''
                argo_index.append(ArgoIndexProfile(
                    fpath, date, lat, lon, ocean, profiler_type, inst, mtime
                ))
        else:
            raise ValueError(u'Unknown format {0}'.format(precedent_format))

    args.output.write(str(argo_index))


def report_ctd_profiles_cruise(args):
    """Return the number of CTD profiles that a cruise has.

    """
    cruises = []
    with closing(args.cruises) as fff:
        for cruise in fff:
            cruises.append(cruise.rstrip())

    sftp = SFTP()
    sftp.connect(get_datadir_hostname())
    aftp = AFTP(sftp)

    with closing(lsession()) as sesh:
        for cruise in cruises:
            if not cruise:
                print ''
                continue
            ctdzipex = sesh.query(Document).filter(Document.ExpoCode == cruise).filter(
                Document.FileType == 'Exchange CTD (Zipped)').first()
            if ctdzipex is None:
                print cruise, sesh.query(Document.FileType).filter(Document.ExpoCode == cruise).all()
            else:
                with aftp.dl(ctdzipex.FileName) as fff:
                    zfile = ZipFile(fff, 'r')
                    print cruise, ctdzipex.FileName, len(zfile.namelist())


def report_profiles_available(args):
    """
    """
    args.output.write('CCHDO Cruises inventory\n')
    args.output.write(','.join([
        'country', 'dtime', 'ship', 'area', 'expocode', 'pi',
        'number of stations', 'data_types', 'param_list']) + '\n')
    with closing(lsession()) as session:
        cruises = session.query(Cruise).order_by(Cruise.Country.asc(), Cruise.Begin_Date.desc()).all()
        for cruise in cruises:
            args.output.write(','.join(map(str, [
                cruise.Country, cruise.Begin_Date, cruise.Ship_Name,
                cruise.Line, cruise.ExpoCode, cruise.Chief_Scientist])))
            args.output.write('\n')

