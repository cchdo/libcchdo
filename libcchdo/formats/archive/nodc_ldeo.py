import os.path
from tarfile import open as tarfile_open

from libcchdo.log import LOG
from libcchdo.model.datafile import DataFile
from libcchdo.formats import ldeo_asep
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = []


def get_filename(basename):
    """Return the filename for this format given a base filename.

    This is a basic implementation using filename extensions.

    """
    return get_filename_fnameexts(basename, _fname_extensions)


def is_filename_recognized(fname):
    """Return whether the given filename is a match for this file format.

    This is a basic implementation using filename extensions.

    """
    return is_filename_recognized_fnameexts(fname, _fname_extensions)


def is_file_recognized(fileobj):
    """Return whether the file is recognized based on its contents.

    This is a basic non-implementation.

    """
    return is_file_recognized_fnameexts(fileobj, _fname_extensions)


def lexico(str_int):
    try:
        return int(str_int)
    except TypeError:
        return str_int


def read(self, fileobj):
    """How to read LDEO ASEP files from an NODC accession."""
    def is_fname_ok(fname):
        if '.csv' not in fname:
            return False
        if fname.find('/') > -1:
            raise ValueError(
                u'CTD Exchange Zip files should not contain directories.')
        return True

    def reader(dfile, fileobj, retain_order, header_only):
        ctdex.read(dfile, fileobj, retain_order, header_only)
        dfile.globals['_FILENAME'] = fileobj.name


    dfiles = []

    datapath = None
    datadirname = '0-data'
    with tarfile_open(mode='r:gz', fileobj=fileobj) as fff:
        for member in fff.getmembers():
            if datapath is None:
                if datadirname in member.name:
                    datapath = member.name.split(datadirname)[0] + datadirname + '/'
                    LOG.info('NODC accession data path: {0}'.format(datapath))
                else:
                    continue

            if not member.name.startswith(datapath):
                continue
            bname = os.path.basename(member.name)
            if bname.endswith('pdf'):
                continue
            if '_ros.' in bname:
                continue
            # don't want upcasts
            if '_ctd_U.' in bname:
                continue

            dfile = DataFile()
            ggg = fff.extractfile(member)
            if ggg is None:
                LOG.error(u'Unable to extract file {0!r}'.format(member))
            else:
                ldeo_asep.read(dfile, ggg)
                dfiles.append(dfile)

    self.files = sorted(
        dfiles, key=lambda dfile: lexico(dfile.globals['STNNBR']))
