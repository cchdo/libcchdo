import zipfile
import datetime
import re

from libcchdo import LOG, pyStringIO
from libcchdo.model.datafile import DataFile
from libcchdo.formats import zip as Zip
from libcchdo.formats.ctd import exchange as ctdex


def read(self, handle, retain_order=False):
    """How to read CTD Exchange files from a Zip."""
    zip = zipfile.ZipFile(handle, 'r')
    for filename in zip.namelist():
        if '.csv' not in filename: continue
        if filename.find('/') > -1:
            LOG.critical(('CTD Exchange Zip files should not contain '
                          'directories. Offending file name: %s') % filename)
            raise ValueError('CTD Exchange Zip files should not contain '
                             'directories. Please ensure you gave a CTD '
                             'Exchange Zip file to be read.')
        tempstream = pyStringIO(zip.read(filename))
        tempstream.name = filename
        ctdfile = DataFile()
        ctdex.read(ctdfile, tempstream, retain_order)
        self.files.append(ctdfile)
        tempstream.close()
    zip.close()


def write(self, handle):
    """How to write CTD Exchange files to a Zip."""
    zip = Zip.create(handle)
    for file in self:
        tempstream = StringIO()
        ctdex.write(file, tempstream)

        station = file.globals['STNNBR'].strip()
        try:
            station = '%05d' % int(station)
        except:
            station = station[:5]

        cast = file.globals['CASTNO'].strip()
        try:
            cast = '%05d' % int(cast)
        except:
            cast = cast[:5]

        filename = '%s_%5s_%5s_ct1.csv' % \
            (file.globals['EXPOCODE'], station, cast)
        filename = re.sub('\s', '_', filename)

        info = zipfile.ZipInfo(filename)
        dt = datetime.datetime.now()
        info.date_time = (dt.year, dt.month, dt.day,
                          dt.hour, dt.minute, dt.second)
        info.external_attr = 0644 << 16L
        info.compress_type = zipfile.ZIP_DEFLATED

        zip.writestr(info, tempstream.getvalue())
        tempstream.close()
    zip.close()
