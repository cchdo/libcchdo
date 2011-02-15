import StringIO
import zipfile


class MemZipFile(zipfile.ZipFile):
    """A modified ZipFile that operates in memory. 
       Handy for writing zip files to streams that can't be seeked.
    """
    def __init__(self, handle, *args, **kwargs):
        self._handle = handle
        self._mem = StringIO.StringIO()
        return zipfile.ZipFile.__init__(self, self._mem, *args, **kwargs)

    def close(self):
        try:
            self._mem
        except AttributeError:
            return
        return_value = zipfile.ZipFile.close(self)
        self._handle.write(self._mem.getvalue())
        self._mem.close()
        del self._mem
        return return_value


def create(handle):
    try:
        return MemZipFile(handle, 'w', zipfile.ZIP_DEFLATED)
    except RuntimeError:
        LOG.info('Unable to write deflated zip file. Using store algorithm instead.')
        return MemZipFile(handle, 'w')
