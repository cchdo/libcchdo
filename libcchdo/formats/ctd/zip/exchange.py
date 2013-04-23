import re
from tempfile import NamedTemporaryFile

from libcchdo.log import LOG
from libcchdo.model.datafile import DataFile
from libcchdo.formats import zip as Zip
from libcchdo.formats.ctd import exchange as ctdex


def read(self, handle, retain_order=False, header_only=False):
    """How to read CTD Exchange files from a Zip.

    The original filenames for each CTD file are included as the global
    _FILENAME on each individual CTD file.

    """
    zip = Zip.ZeroCommentZipFile(handle, 'r')
    for filename in zip.namelist():
        if '.csv' not in filename: continue
        if filename.find('/') > -1:
            LOG.critical(('CTD Exchange Zip files should not contain '
                          'directories. Offending file name: %s') % filename)
            raise ValueError('CTD Exchange Zip files should not contain '
                             'directories. Please ensure you gave a CTD '
                             'Exchange Zip file to be read.')
        with NamedTemporaryFile(prefix=filename) as tempfile:
            tempfile.write(zip.read(filename))
            tempfile.flush()
            tempfile.seek(0)
            ctdfile = DataFile()
            ctdex.read(ctdfile, tempfile, retain_order, header_only)
            ctdfile.globals['_FILENAME'] = filename
            self.append(ctdfile)
    zip.close()


def get_filename(expocode, station, cast):
    """Filename for Exchange CTD files."""
    station = station.strip()
    try:
        station = '%05d' % int(station)
    except TypeError:
        station = station[:5]

    cast = cast.strip()
    try:
        cast = '%05d' % int(cast)
    except TypeError:
        cast = cast[:5]

    filename = '%s_%5s_%5s_ct1.csv' % (expocode, station, cast)
    filename = re.sub('\s', '_', filename)
    return filename


def get_datafile_filename(dfile):
    expocode = dfile.globals['EXPOCODE']
    station = dfile.globals['STNNBR'].strip()
    cast = dfile.globals['CASTNO'].strip()
    return get_filename(expocode, station, cast)


def write(self, handle):
    """How to write CTD Exchange files to a Zip."""
    Zip.write(self, handle, ctdex, get_datafile_filename)
