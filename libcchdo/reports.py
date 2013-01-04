from datetime import datetime, timedelta
from contextlib import closing

from sqlalchemy.sql import not_, between

from libcchdo import LOG
from libcchdo.db.model import legacy
from libcchdo.db.model.legacy import (
    Document, Cruise, Submission, QueueFile,
    )


types_to_ignore = [
    'Coord info', 'GMT info File', 'Large Plot', 'Postscript file',
    'Small Plot', 'Unrecognized', 'Directory', 'Coordinates?',
]


def report_data_updates(args):
    """Counts updates within the time frame.

    Provide a summary of:
    * number of modifications to each file type
    * number of cruises with updated files

    """
    with closing(legacy.session()) as session:
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
        type_edit_counts = {}
        type_add_counts = {}
        cruises = set()
        for doc in docs:
            if 'original' in doc.FileName or 'Queue' in doc.FileName:
                continue
            details = [doc.LastModified, doc.ExpoCode, doc.FileName]
            LOG.info(' '.join(map(str, details)))
            try:
                type_add_counts[doc.FileType] += 1
            except KeyError:
                type_add_counts[doc.FileType] = 1
            if not doc.Modified:
                continue
            for mtime in doc.Modified.split(','):
                mtime = datetime.strptime(mtime, '%Y-%m-%d %H:%M:%S')
                if date_start < mtime and mtime < date_end:
                    LOG.info('\t{0}\n'.format(mtime))
                    cruises.add(doc.ExpoCode)
                    try:
                        type_edit_counts[doc.FileType] += 1
                    except KeyError:
                        type_edit_counts[doc.FileType] = 1
                else:
                    pass
                    LOG.info('\t{0} out of range\n'.format(mtime))
        args.output.write(
            'Data updates from {0}/{1}:\n'.format(date_start, date_end))
        args.output.write(
            '# cruises supported: {0}\n'.format(session.query(Cruise).count()))
        args.output.write(
            '# cruises with updated files: {0}\n'.format(len(cruises)))
        args.output.write(
            '# files added: {0}\n'.format(sum(type_add_counts.values())))
        args.output.write(
            '# file updates: {0}\n'.format(sum(type_edit_counts.values())))
        args.output.write('File type add counts:\n')
        args.output.write(repr(type_add_counts) + '\n')
        args.output.write('File type edit counts:\n')
        args.output.write(repr(type_edit_counts) + '\n')
        args.output.write('Cruises with updated files:\n')
        args.output.write(repr(sorted(list(cruises))) + '\n')


def report_submission_and_queue(args):
    """Counts submissions and queue updates.

    Provide a summary of:
    * number of submissions
    * number of queue updates

    """
    with closing(legacy.session()) as session:
        date_end = args.date_end
        date_start = args.date_start
        args.output.write('/'.join(map(str, [date_start, date_end])) + '\n')

        submissions = session.query(Submission).\
            filter(
                between(
                    Submission.submission_date, date_start, date_end)).\
            filter(Submission.email != 'tooz@oceanatlas.com').\
            count()

        submissions_assimilated = session.query(Submission).\
            filter(
                between(
                    Submission.submission_date, date_start, date_end)).\
            filter(Submission.email != 'tooz@oceanatlas.com').\
            filter(Submission.assimilated == True).\
            count()

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
            '# queued: {0}\n'.format(queued))
        args.output.write(
            '# queued and merged: {0}\n'.format(queued_and_merged))
        args.output.write(
            '# queued and not merged: {0}\n'.format(queued - queued_and_merged))
