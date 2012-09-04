from datetime import datetime, timedelta
from contextlib import closing

from sqlalchemy.sql import not_, between

from libcchdo.db.model import legacy
from libcchdo.db.model.legacy import Document


types_to_ignore = [
    'Coord info', 'Documentation', 'GMT info File', 'Large Plot',
    'Postscript file', 'Small Plot', 'Unrecognized',
]


def report_data_types_changed(args):
    with closing(legacy.session()) as session:
        date_end = datetime(2012, 7, 1)
        date_start = date_end - timedelta(365)

        docs = session.query(Document).\
            filter(
                between(
                    Document.LastModified, date_start, datetime.utcnow())).\
            filter(not_(Document.FileType.in_(types_to_ignore))).\
            all()

        # count modifications of file types

        counts = {}
        for doc in docs:
            if 'original' in doc.FileName or 'Queue' in doc.FileName:
                continue
            args.output.write(str(doc.LastModified) + ' ' + str(doc.FileName) + '\n')
            for mtime in doc.Modified.split(','):
                mtime = datetime.strptime(mtime, '%Y-%m-%d %H:%M:%S')
                if date_start < mtime and mtime < date_end:
                    args.output.write('\t{0}\n'.format(mtime))
                    try:
                        counts[doc.FileType] += 1
                    except KeyError:
                        counts[doc.FileType] = 1
                else:
                    args.output.write('\t{0} out of range\n'.format(mtime))
        args.output.write(repr(counts) + '\n')
