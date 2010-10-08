#!/usr/bin/env python


from __future__ import with_statement
import logging
import os.path
import struct
import sys

import abs_import_library
import libcchdo.model.datafile as datafile
import libcchdo.formats.summary.woce as sumwoce
import libcchdo.formats.bottle.exchange as botex
import libcchdo.datadir.util


def ensure_nav(root, dirs, files):
    navfiles = filter(lambda f: f.endswith('na.txt'), files)
    if len(navfiles) > 0:
        logging.info('%s has nav files %s' % (root, ', '.join(navfiles)))
    else:
        logging.info(("%s is missing a nav file. "
                      "Attempting to generate one.") % root)
    # Try to use easiest generation method first
    generation_methods = [
        ['Bottle Exchange', 'hy1.csv', botex.read],
        ['Summary', 'su.txt', sumwoce.read],
        # Other WOCE files do not have lng lat (they're in the Summary file)
        # TODO Collections have to have some regular way to be merged before
        # they can be outputted to nav.
        #['CTD Exchange', 'ct1.zip',
        #  datafile.DataFileCollection.read_CTDZip_Exchange],
        #['Bottle NetCDF', 'nc_hyd.zip',
        #  datafile.DataFileCollection.read_BottleZip_NetCDF],
        #['CTD NetCDF', 'nc_ctd.zip',
        #  datafile.DataFileCollection.read_CTDZip_NetCDF],
    ]
    for methodname, extension, readfn in generation_methods:
        basefiles = filter(lambda f: f.endswith(extension), files)
        if len(basefiles) > 0:
            logging.info('  Found a %s file.' % methodname)
            for file in basefiles:
                try:
                    outputfile = '%sna.txt' % file[:-len(extension)]
                    logging.info('  Generating nav file %s from a %s file %s.' % \
                         (outputfile, methodname, file))
                    fh = readfn.im_class()
                    with open(os.path.join(root, file), 'r') as in_file:
                        readfn(fh, in_file)
                    #with open(os.path.join(root, outputfile), 'w') as out_file:
                    #  fh.write_nav(out_file)
                    from sys import stdout
                    print fh
                    fh.write_nav(stdout)
                    return True
                except NotImplementedError, e:
                    logging.info(("Unable to generate. The read function has not been "
                                  "implemented: %s") % e)
                except struct.error, e1:
                    logging.info(("  Ignoring WOCE unpack error and continuing with "
                                  "different method: %s") % e1)
                except NameError, e2:
                    if str(e2).endswith("not in CCHDO's parameter list."):
                        logging.info('  Ignoring parameter not in database error.')
                    else:
                        logging.warning('  Ignoring exception: %s' % e2)
                except ValueError, e3:
                    if str(e3).startswith("time data did not match format"):
                        logging.info('  Ignoring time data format error: %s' % e3)
                    else:
                        logging.warning('  Ignoring exception: %s' % e3)
                except Exception, ee:
                    logging.warning('  Ignoring exception: %s' % ee)
        logging.info('  Unable to find a %s file.' % methodname)
    logging.warning('  Unable to generate a nav file for %s' % root)
    return False


if __name__ == '__main__':
    libcchdo.datadir.util.do_for_cruise_directories(ensure_nav)
