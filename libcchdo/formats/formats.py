"""Dealing with file formats.

This module deals with how data files are named (with extensions) and how they
are mapped to read/write modules.

"""
import os.path
import sys
from collections import OrderedDict
from pkgutil import walk_packages, iter_modules

import libcchdo.formats
from libcchdo.log import LOG


def get_filename_fnameexts(basename, exts):
    """Return the filename for this format given a base filename.

    This is a basic implementation using filename extensions.

    """
    return basename + exts[0]


def is_filename_recognized_fnameexts(fname, exts):
    """Return whether the given filename is a match for this file format.

    This is a basic implementation using filename extensions.

    """
    return any(fname.endswith(suffix) for suffix in exts)


def is_file_recognized_fnameexts(fileobj, exts):
    """Return whether the file is recognized based on its contents.

    This is a basic non-implementation.

    """
    raise NotImplementedError()


def short_name(module):
    trimmed_name = '.'.join(module.__name__.split('.')[2:])
    trimmed_name = trimmed_name.replace('bottle', 'btl')
    trimmed_name = trimmed_name.replace('netcdf', 'nc')
    trimmed_name = trimmed_name.replace('summary', 'sum')
    trimmed_name = trimmed_name.replace('exchange', 'ex')
    trimmed_name = trimmed_name.replace('oceansites', 'os')
    trimmed_name = trimmed_name.replace(
        'bermuda_atlantic_time_series_study', 'bats')
    return trimmed_name


class ShieldedDict(OrderedDict):
    _overridden_names = [
        '__getitem__', 'items', 'keys', 'values', '__str__', '__repr__']

    def __init__(self, parent, *args):
        super(ShieldedDict, self).__init__(*args)
        self._parent = parent
        self._unshielded = False

    def __getattribute__(self, name):
        if name not in OrderedDict.__getattribute__(self, '_overridden_names'):
            return OrderedDict.__getattribute__(self, name)
        def _missing(*args, **kwargs):
            if not self._unshielded:
                self._unshield()
            return OrderedDict.__getattribute__(self, name)()
        return _missing

    def _unshield(self):
        self._parent._scan_if_needed()

class FormatScanner(object):
    def __init__(self, *args):
        super(FormatScanner, self).__init__(*args)

        self._file_extensions = FileExtensions(self, [
            ['coriolis', ['coriolis']],
        ])
        self._all_formats = FileTypeModule(self)

        # TODO How to handle file extension collisions?

    @property
    def file_extensions(self):
        return self._file_extensions

    @property
    def all_formats(self):
        return self._all_formats

    def _scan_if_needed(self):
        try:
            self._scanning
        except AttributeError:
            self._scanning = True
            self._scan_for_formats(libcchdo.formats)

    def _scan_for_formats(self, root):
        """Scan sub-package for format modules."""
        prefix = root.__name__ + '.'
        for loader, name, ispkg in walk_packages(root.__path__, prefix=prefix):
            # Don't load this same module during the scan
            if name == 'libcchdo.formats.formats':
                continue

            modname = '.'.join(name.split('.')[:-1])
            basename = name.split('.')[-1]

            module = loader.find_module(name)
            if not module:
                # Error! couldn't find the module to load
                print 'No module found for', name
                continue
            try:
                loaded_module = module.load_module(name)
            except ImportError, err:
                LOG.error(u'Unable to load format module {0}:\n{1!r}'.format(
                    name, err))
                continue
            sys.modules[name] = loaded_module
            sys.modules[modname].__setattr__(basename, loaded_module)
            loaded_module.__name__ = name
            shortname = short_name(loaded_module)
            try:
                loaded_module.get_filename
                loaded_module.is_filename_recognized
                loaded_module.is_file_recognized

                self.file_extensions[shortname] = loaded_module._fname_extensions
            except AttributeError, err:
                #LOG.info('Not a format module {0}: {1!r}'.format(name, err))
                pass

            # A fully defined format module must have either a read or write
            if (    hasattr(loaded_module, 'read') or
                    hasattr(loaded_module, 'write')):
                self.all_formats[shortname] = loaded_module


class FileExtensions(ShieldedDict):
    pass


class FileTypeModule(ShieldedDict):
    def __getitem__(self, key):
        if type(key) is not str:
            LOG.debug(repr(key))
            return key
        module = super(FileTypeModule, self).__getitem__(key)
        if type(module) is not str:
            return module
        try:
            __import__(module)
            return sys.modules[module]
        except ImportError:
            return None


_formats = FormatScanner()


file_extensions = _formats.file_extensions
    

all_formats = _formats.all_formats


def guess_file_type(filename, file_type=None):
    if file_type is not None:
        return file_type

    for fmt, exts in file_extensions.items():
        for ext in exts:
            if filename.endswith(ext):
                return fmt
    # TODO use is_filename_recognized and is_file_recognized
    return None


def read_arbitrary(handle, file_type=None, file_name=None):
    '''Takes any CCHDO recognized file and tries to open it.
       The recognition is done by file extension.
       Args:
           handle - a file handle
           file_type - forces a specific reader to be used
       Returns:
           a DataFile(Collection) or *SummaryFile that matches the file type.
    '''
    try:
        handle.read
    except AttributeError:
        raise ValueError(u'read_arbitrary must be called with a file object')
    from libcchdo.model.datafile import (
        DataFile, SummaryFile, DataFileCollection)

    if not file_name:
        try:
            file_name = handle.name
        except AttributeError:
            pass

    file_type = guess_file_type(file_name, file_type)

    if file_type is None:
        raise ValueError('Unrecognized file type for %s' % handle)

    if 'zip' in file_type:
        dfile = DataFileCollection()
    elif file_type.startswith('sum'):
        dfile = SummaryFile()
    else:
        dfile = DataFile()
    
    if file_type == 'sd2':
        file_type = 'nodc_sd2'

    try:
        format_module = all_formats[file_type]
    except KeyError:
        raise ValueError('Unrecognized file type for %s' % handle.name)
    format_module.read(dfile, handle)

    return dfile
