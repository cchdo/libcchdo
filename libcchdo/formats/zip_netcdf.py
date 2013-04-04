"""Common code for manipulating netCDF Zip archives."""
from tempfile import SpooledTemporaryFile

from libcchdo.model.datafile import DataFile
from libcchdo.formats import netcdf as nc, zip as Zip


def read(self, handle, reader):
    """Generic reader for netCDF files in zip."""
    zfile = Zip.ZeroCommentZipFile(handle, 'r')
    try:
        for fname in zfile.namelist():
            if not fname.endswith('.nc'):
                continue
            with SpooledTemporaryFile(max_size=2 ** 13) as tempfile:
                tempfile.write(zfile.read(fname))
                tempfile.flush()
                tempfile.seek(0)
                dfile = DataFile()
                reader.read(dfile, tempfile)
                self.files.append(dfile)
    finally:
        zfile.close()


def get_identifier_btl(dfile):
    """Return a tuple containing the ExpoCode, station and cast for a BTL file.

    """
    expocode = dfile['EXPOCODE'][0] or 'UNKNOWN'
    station = dfile['STNNBR'][0]
    cast = dfile['CASTNO'][0]
    return (expocode, station, cast)


def get_identifier_ctd(dfile):
    """Return a tuple containing the ExpoCode, station and cast for a CTD file.

    """
    expocode = dfile.globals.get('EXPOCODE', 'UNKNOWN')
    station = dfile.globals.get('STNNBR')
    cast = dfile.globals.get('CASTNO')
    return (expocode, station, cast)


def write(self, handle, extension, writer, get_identifier_func):
    """How to write netCDF files to a Zip.

    get_identifier_func(DataFile) - 
        called to get a tuple containing (expocode, station, cast)

    If no station or cast is available, the writer will simply count up starting
    from 1.

    """
    station_i = 0
    cast_i = 0
    def get_filename(dfile):
        expocode, station, cast = get_identifier_func(dfile)
        if station is None:
            station = station_i
            station_i += 1
        if cast is None:
            cast = cast_i
            cast_i += 1
        return nc.get_filename(expocode, station, cast, extension)

    Zip.write(self, handle, writer, get_filename)
