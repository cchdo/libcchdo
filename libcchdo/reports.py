from datetime import datetime, timedelta
from contextlib import closing

from sqlalchemy.sql import not_, between

from libcchdo.db.model import legacy
from libcchdo.db.model.legacy import Document


types_to_ignore = [
    'Coord info', 'GMT info File', 'Large Plot', 'Postscript file',
    'Small Plot', 'Unrecognized',
]


def report_data_updates(args):
    """Counts updates within the time frame.

    Provide a summary of:
    * number of modifications to each file type
    * number of cruises with updated files

    """
    with closing(legacy.session()) as session:
        today = datetime.utcnow()
        date_end = datetime(today.year, 7, 1)
        date_start = date_end - timedelta(366)
        args.output.write('/'.join(map(str, [date_start, date_end])) + '\n')

        docs = session.query(Document).\
            filter(
                between(
                    Document.LastModified, date_start, datetime.utcnow())).\
            filter(not_(Document.FileType.in_(types_to_ignore))).\
            all()

        # count modifications of file types
        counts = {}
        cruises = set()
        for doc in docs:
            if 'original' in doc.FileName or 'Queue' in doc.FileName:
                continue
            details = [doc.LastModified, doc.ExpoCode, doc.FileName]
            args.output.write(' '.join(map(str, details)) + '\n')
            for mtime in doc.Modified.split(','):
                mtime = datetime.strptime(mtime, '%Y-%m-%d %H:%M:%S')
                if date_start < mtime and mtime < date_end:
                    args.output.write('\t{0}\n'.format(mtime))
                    cruises.add(doc.ExpoCode)
                    try:
                        counts[doc.FileType] += 1
                    except KeyError:
                        counts[doc.FileType] = 1
                else:
                    args.output.write('\t{0} out of range\n'.format(mtime))
        args.output.write(repr(counts) + '\n')
        args.output.write(repr(sorted(list(cruises))) + '\n')
        args.output.write(str(len(cruises)) + '\n')
