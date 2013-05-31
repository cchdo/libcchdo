"""Exchange related functions.

"""
from re import compile as re_compile, match as re_match

from libcchdo.log import LOG


r_idstamp = re_compile('(BOTTLE|CTD),(\w+)')
r_stamp = re_compile('\d{8}\w+')


def read_identifier_line(dfile, fileobj, ftype):
    """Read an Exchange identifier line from the fileobj to the dfile.

    An Exchange identifier line begins with either "BOTTLE," or "CTD," and ends
    with a WOCE style stamp.

    Raises:
        ValueError - if either the file type is not one of BOTTLE or CTD or the
            identifier line is malformed.

    """
    stamp_line = fileobj.readline()
    matchgrp = r_idstamp.match(stamp_line)
    if not matchgrp:
        raise ValueError(
            u"Expected Exchange type identifier line with stamp (e.g. "
            "{0},YYYYMMDDdivINSwho) got: {1!r}".format(ftype, stamp_line))

    read_ftype = matchgrp.group(1)
    if ftype != read_ftype:
        raise ValueError(
            u'Expected Exchange file type {0!r} and got {1!r}'.format(
            ftype, read_ftype))
    stamp = dfile.globals['stamp'] = matchgrp.group(2)
    if not r_stamp.match(stamp):
        LOG.warn(u'{0!r} does not match stamp format YYYYMMDDdivINSwho.'.format(
            stamp))

def read_comments(dfile, fileobj):
    """Read the Exchange header comments.

    Comments are contiguous lines starting with '#'.

    Return: the last line that was read and determined as not a comment.

    """
    line = fileobj.readline()
    headers = []
    while line and line.startswith('#'):
        # It's possible for files to come in with unicode.
        headers.append(line.decode('raw_unicode_escape'))
        line = fileobj.readline()
    dfile.globals['header'] = u''.join(headers)
    return line
