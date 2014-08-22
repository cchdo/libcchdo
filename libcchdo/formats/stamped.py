"""Stamped formats.

Formats such as Exchange and WHP netCDF have what are called "stamps".

"""
from zipfile import is_zipfile, ZipFile
from logging import getLogger


log = getLogger(__name__)


from libcchdo.formats.zip import generate_files as zip_gen_files


def read_stamp(fileobj, reader):
    """Only get the file type and stamp line.

    For zipfiles, return the most common stamp and warn if there is more than
    one.

    """
    if is_zipfile(fileobj):
        all_stamps = {}
        for member in zip_gen_files(fileobj):
            key = tuple(reader(member))
            try:
                all_stamps[key] += 1
            except KeyError:
                all_stamps[key] = 1
        if len(all_stamps) > 1:
            stamps = sorted(
                all_stamps.items(), key=lambda x: x[1], reverse=True)
            log.warn(u'Zip file has more than one stamp:\n{0!r}'.format(stamps))
            stamp = stamps[0][0]
            log.info(u'Picked stamp with most occurences: {0}'.format(stamp))
        elif len(all_stamps) == 0:
            stamp = (None, None)
        else:
            stamp = all_stamps.keys()[0]
        return stamp
    else:
        fileobj.seek(0)
        return reader(fileobj)
