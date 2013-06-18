"""Formats

This subpackage is organized into modules that either provide read/write
capabilities or support those processes.

Format modules
--------------

Reading/Writing
===============

format modules (formats) are allowed to specify two operations::

* read(DataFile_or_Collection, file_object)
* write(DataFile_or_Collection, file_object)

Formats are *not* required to specify both reading and writing.

File names
==========

If a format is to be recognized from either its filename or contents, its module
is *required* to specify three functions::

* get_filename()
* is_filename_recognized(fname)
* is_file_recognized(fname)

If these functions are not defined, the format will be available in
`all_formats` but not `file_extensions`.


Hierarchy
---------

Formats are specified in the formats package broken into a hierarchy based on
the type of file (bottle, CTD, etc), whether it is a collection of files, and
the format of the type of file (WOCE, Exchange, etc).

Example
=======

Convert CTD Exchange file into CTD NetCDF file::

    # Since CTD Exchange is actually a Zip file containing single CTD casts one
    # expects to find the format under formats.ctd.zip.exchange.
    import formats.ctd.zip.exchange as ctdzipex
    
    # The same follows for NetCDF (a collection of smaller .nc files)
    import formats.ctd.zip.netcdf as ctdzipnc
    
    import models.datafile
    
    
    dfc = models.datafile.DataFileCollection()
    
    with open(ctd_exchange_file_path, 'r') as fh:
        ctdzipex.read(dfc, fh)
    with open(ctd_netcdf_file_path, 'w') as fh:
        ctdzipnc.write(dfc, fh)
"""


_pre_write_functions = []


# TODO
def pre_write(self):
    """ Should be called by all writers before doing anything. """
    for fn in _pre_write_functions:
    	fn(self)


def add_pre_write(fn):
    _pre_write_functions.append(fn)


def _report_changes(self):
    if self.changes_to_report:
        self.globals['header'] = '\n'.join(
            map(lambda x: '#' + x, self.changes_to_report +
                                   [self.globals['stamp']]) + 
            [self.globals['header']])


add_pre_write(_report_changes)
