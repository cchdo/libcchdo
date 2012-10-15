"""Entry point for libcchdo.

This module is the entry point for the hydro utility. To see a list of all
available sub entry points, run 

$ hydro --help

"""


import argparse
from argparse import RawTextHelpFormatter
from datetime import datetime, date
from contextlib import closing
import sys
import os
import os.path

from libcchdo import LOG

from libcchdo.fns import all_formats
from libcchdo.formats.netcdf_oceansites import (
    OCEANSITES_VERSIONS, OCEANSITES_TIMESERIES)
known_formats = all_formats.keys()


def _qualify_oceansites_type(args):
    if args.timeseries is None:
        LOG.warn(
            u'Printing an AMBIGUOUS (read: INVALID) OceanSITES NetCDF Zip')
    else:
        LOG.info(
            u'Printing a {0} OceanSITES NetCDF Zip'.format(args.timeseries))


hydro_parser = argparse.ArgumentParser(
    description='libcchdo tools',
    formatter_class=RawTextHelpFormatter)


hydro_subparsers = hydro_parser.add_subparsers(
    title='subcommands')


converter_parser = hydro_subparsers.add_parser(
    'convert', help='Format converters')
converter_parsers = converter_parser.add_subparsers(
    title='format converters')


any_converter_parser = converter_parsers.add_parser(
    'any', help='any format converters')
any_converter_parsers = any_converter_parser.add_subparsers(
    title='any format converters')


def any_to_type(args):
    """Convert any recognized CCHDO file to any valid output type."""
    from libcchdo.fns import read_arbitrary, all_formats
    from libcchdo.formats.common import nav
    from libcchdo.formats import google_wire

    with closing(args.cchdo_file) as in_file:
        file = read_arbitrary(in_file, args.input_type)

    with closing(args.output) as out_file:
        if args.output_type == 'nav':
            nav.write(file, out_file)
        elif args.output_type == 'google_wire':
            google_wire.write(file, out_file, json=args.json)
        elif args.output_type == 'dict':
            out_file.write(str(file.to_dict()))
            out_file.write('\n')
        elif args.output_type == 'str':
            out_file.write(str(file))
            out_file.write('\n')
        else:
            try:
                format = all_formats[args.output_type]
            except (KeyError, ImportError):
                LOG.error('Unrecognized format %s' % args.output_type)
                return 1
            format.write(file, out_file)


any_to_type_parser = any_converter_parsers.add_parser(
    'type',
    help=any_to_type.__doc__)
any_to_type_parser.set_defaults(
    main=any_to_type)
any_to_type_parser.add_argument('-t', '--output-type', '--type',
    choices=['str', 'dict', 'google_wire', 'nav', ] + known_formats,
    default='str', help='output types (default: str)')
any_to_type_parser.add_argument('-i', '--input-type', choices=known_formats,
    help='force the input file to be read as the specified type')
any_to_type_parser.add_argument('-j', '--json', action='store_true',
    help='only applies to output type google_wire. Forces the google_wire '
         'output to be valid JSON.')
any_to_type_parser.add_argument(
    'cchdo_file', type=argparse.FileType('r'),
     help='any recognized CCHDO file')
any_to_type_parser.add_argument(
    'output', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
     help='output file (default: stdout)')


def any_to_db_track_lines(args):
    from libcchdo.fns import read_arbitrary
    from libcchdo.formats.common import track_lines

    with closing(args.input_file) as in_file:
        data = read_arbitrary(in_file, args.input_type)
    
    with closing(args.output_track_lines) as out_file:
        track_lines.write(data, out_file)


any_to_db_track_lines_parser = any_converter_parsers.add_parser(
    'db_track_lines',
    help=any_to_db_track_lines.__doc__)
any_to_db_track_lines_parser.set_defaults(
    main=any_to_db_track_lines)
any_to_db_track_lines_parser.add_argument('-i', '--input-type',
    choices=known_formats,
    help='force the input file to be read as the specified type')
any_to_db_track_lines_parser.add_argument(
    'input_file', type=argparse.FileType('r'),
    help='any recognized CCHDO file')
any_to_db_track_lines_parser.add_argument(
    'output_track_lines', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output track lines file (default: stdout)')


bot_converter_parser = converter_parsers.add_parser(
    'bottle', help='Bottle format converters')
bot_converter_parsers = bot_converter_parser.add_subparsers(
    title='bottle format converters')


def bottle_exchange_to_db(args):
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.bottle.exchange as botex
    import libcchdo.formats.bottle.database as botdb

    df = DataFile()
    
    with closing(args.input_botex) as in_file:
        botex.read(df, in_file)

    botdb.write(df)


bottle_exchange_to_db_parser = bot_converter_parsers.add_parser(
    'exchange_to_db',
    help=bottle_exchange_to_db.__doc__)
bottle_exchange_to_db_parser.set_defaults(
    main=bottle_exchange_to_db)
bottle_exchange_to_db_parser.add_argument(
    'input_botex', type=argparse.FileType('r'),
    help='input Bottle Exchange file')


def bottle_exchange_to_kml(args):
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.bottle.exchange as botex
    from libcchdo.kml import bottle_exchange_to_kml

    df = DataFile()
    
    with closing(args.input_botex) as in_file:
        botex.read(df, in_file)

    with closing(args.output) as out_file:
        bottle_exchange_to_kml(df, out_file)


bottle_exchange_to_kml_parser = bot_converter_parsers.add_parser(
    'exchange_to_kml',
    help=bottle_exchange_to_kml.__doc__)
bottle_exchange_to_kml_parser.set_defaults(
    main=bottle_exchange_to_kml)
bottle_exchange_to_kml_parser.add_argument(
    'input_botex', type=argparse.FileType('r'),
    help='input Bottle Exchange file')
bottle_exchange_to_kml_parser.add_argument(
    'output', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output file (default: stdout)')


def bottle_exchange_to_parameter_kml(args):
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.bottle.exchange as botex
    from libcchdo.kml import bottle_exchange_to_parameter_kml

    df = DataFile()
    
    with closing(args.input_botex) as in_file:
        botex.read(df, in_file)

    with closing(args.output) as out_file:
        bottle_exchange_to_parameter_kml(df, out_file)


bottle_exchange_to_parameter_kml_parser = bot_converter_parsers.add_parser(
    'exchange_to_parameter_kml',
    help=bottle_exchange_to_parameter_kml.__doc__)
bottle_exchange_to_parameter_kml_parser.set_defaults(
    main=bottle_exchange_to_parameter_kml)
bottle_exchange_to_parameter_kml_parser.add_argument(
    'input_botex', type=argparse.FileType('r'),
    help='input Bottle Exchange file')
bottle_exchange_to_parameter_kml_parser.add_argument(
    'output', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output file (default: stdout)')


def bottle_exchange_to_bottlezip_netcdf(args):
    from libcchdo.model.datafile import DataFile
    import libcchdo.model.convert.datafile_to_datafilecollection as df2dfc
    import libcchdo.formats.bottle.exchange as botex
    import libcchdo.formats.bottle.zip.netcdf as botzipnc

    df = DataFile()

    with closing(args.input_botex) as in_file:
        botex.read(df, in_file)

    with closing(args.output_botzipnc) as out_file:
        botzipnc.write(df2dfc.split_bottle(df), out_file)


bottle_exchange_to_bottlezip_netcdf_parser = bot_converter_parsers.add_parser(
    'exchange_to_zip_netcdf',
    help=bottle_exchange_to_bottlezip_netcdf.__doc__)
bottle_exchange_to_bottlezip_netcdf_parser.set_defaults(
    main=bottle_exchange_to_bottlezip_netcdf)
bottle_exchange_to_bottlezip_netcdf_parser.add_argument(
    'input_botex', type=argparse.FileType('r'),
    help='input Bottle Exchange file')
bottle_exchange_to_bottlezip_netcdf_parser.add_argument(
    'output_botzipnc', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output Bottle ZIP NetCDF file (default: stdout)')


def bottle_woce_and_summary_woce_to_bottle_exchange(args):
    from libcchdo.model.datafile import DataFile, SummaryFile
    from libcchdo.formats import woce
    import libcchdo.formats.summary.woce as sumwoce
    import libcchdo.formats.bottle.woce as botwoce
    import libcchdo.formats.bottle.exchange as botex

    bottlefile = DataFile()
    sumfile = SummaryFile()

    with closing(args.botwoce) as in_file:
        botwoce.read(bottlefile, in_file)

    with closing(args.sumwoce) as in_file:
        sumwoce.read(sumfile, in_file)

    woce.combine(bottlefile, sumfile)

    with closing(args.botex) as out_file:
        botex.write(bottlefile, out_file)


bottle_woce_and_summary_woce_to_bottle_exchange_parser = \
    bot_converter_parsers.add_parser(
        'woce_and_summary_woce_to_exchange',
        help=bottle_woce_and_summary_woce_to_bottle_exchange.__doc__)
bottle_woce_and_summary_woce_to_bottle_exchange_parser.set_defaults(
    main=bottle_woce_and_summary_woce_to_bottle_exchange)
bottle_woce_and_summary_woce_to_bottle_exchange_parser.add_argument(
    'botwoce', type=argparse.FileType('r'),
    help='input Bottle WOCE file')
bottle_woce_and_summary_woce_to_bottle_exchange_parser.add_argument(
    'sumwoce', type=argparse.FileType('r'),
    help='input Summary WOCE file')
bottle_woce_and_summary_woce_to_bottle_exchange_parser.add_argument(
    'botex', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output Bottle Exchange file')


ctd_converter_parser = converter_parsers.add_parser(
    'ctd', help='CTD format converters')
ctd_converter_parsers = ctd_converter_parser.add_subparsers(
    title='CTD format converters')


def ctd_bats_to_ctd_exchange(args):
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.ctd.exchange as ctdex
    import libcchdo.formats.ctd.bermuda_atlantic_time_series_study as ctd_bats

    df = DataFile()

    with closing(args.ctdbats) as in_file:
        ctd_bats.read(df, in_file)

    with closing(args.ctdex) as out_file:
        ctdex.write(df, out_file)


ctd_bats_to_ctd_exchange_parser = ctd_converter_parsers.add_parser(
    'bats_to_exchange',
    help=ctd_bats_to_ctd_exchange.__doc__)
ctd_bats_to_ctd_exchange_parser.set_defaults(
    main=ctd_bats_to_ctd_exchange)
ctd_bats_to_ctd_exchange_parser.add_argument(
    'ctdbats', type=argparse.FileType('r'),
    help='input CTD BATS file')
ctd_bats_to_ctd_exchange_parser.add_argument(
    'ctdex', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output CTD Exchange file')


def ctd_exchange_to_ctd_netcdf(args):
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.ctd.exchange as ctdex
    import libcchdo.formats.ctd.netcdf as ctdnc

    df = DataFile()

    with closing(args.ctdex) as in_file:
        ctdex.read(df, in_file)

    with closing(args.ctdnc) as out_file:
        ctdnc.write(df, out_file)


ctd_exchange_to_ctd_netcdf_parser = ctd_converter_parsers.add_parser(
    'exchange_to_netcdf',
    help=ctd_exchange_to_ctd_netcdf.__doc__)
ctd_exchange_to_ctd_netcdf_parser.set_defaults(
    main=ctd_exchange_to_ctd_netcdf)
ctd_exchange_to_ctd_netcdf_parser.add_argument(
    'ctdex', type=argparse.FileType('r'),
    help='input CTD Exchange file')
ctd_exchange_to_ctd_netcdf_parser.add_argument(
    'ctdnc', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output CTD NetCDF file')


def ctd_polarstern_to_ctd_exchange(args):
    import sqlite3
    from libcchdo.tools import ctd_polarstern_to_ctd_exchange

    try:
        db = sqlite3.connect(args.database_file)
    except:
        LOG.error(u"{0} is not a SQLite3 database.".format(args.database_file))
        return 1

    with closing(db) as db:
        ctd_polarstern_to_ctd_exchange(args, db)


ctd_polarstern_to_ctd_exchange_parser = ctd_converter_parsers.add_parser(
    'polarstern_to_exchange',
    help=ctd_polarstern_to_ctd_exchange.__doc__)
ctd_polarstern_to_ctd_exchange_parser.set_defaults(
    main=ctd_polarstern_to_ctd_exchange)
ctd_polarstern_to_ctd_exchange_parser.add_argument(
    '--commit-to-file', type=bool, default=False,
    help='Write to a file')
ctd_polarstern_to_ctd_exchange_parser.add_argument(
    'database_file', type=str,
    help='SQLite3 database containing PolarStern metadata (previously '
         'extracted)')
ctd_polarstern_to_ctd_exchange_parser.add_argument(
    'files', type=str, nargs='+',
    help='The PolarStern data file(s) (*.tab -> *.tab.txt)')
ctd_polarstern_to_ctd_exchange_parser.add_argument(
    'ctdex', type=argparse.FileType('wb'), nargs='?',
    default=sys.stdout,
    help='output CTD Exchange file')


def ctd_sbe_to_ctd_exchange(args):
    """Convert raw ascii seabird ctd files to ctd exchange or ctd zip exchange.

    The channel specifiers use an index number rather than a name because of the
    posibility for channels to have identical names. All calculated parameters
    and non CCHDO recognized parameters (e.g. PAR) are ignored.

    """
    from libcchdo.tools import sbe_to_ctd_exchange

    sbe_to_ctd_exchange(args)


ctd_sbe_to_ctd_exchange_parser = ctd_converter_parsers.add_parser(
    'sbe_to_exchange',
    help=ctd_sbe_to_ctd_exchange.__doc__)
ctd_sbe_to_ctd_exchange_parser.set_defaults(
    main=ctd_sbe_to_ctd_exchange)
ctd_sbe_to_ctd_exchange_parser.add_argument(
    'files', type=file, nargs='+',
    help='File or list of files that will be converted to exchange format, if '
        'a single file is given, a flat exchange file will be output, if more '
        'than one is given, a ctd zip will be output')
ctd_sbe_to_ctd_exchange_parser.add_argument(
    '-s', '--salt',
    help='in the case of multiple salinity channels, the channel may be '
        'chosen by index')
ctd_sbe_to_ctd_exchange_parser.add_argument(
    '-t', '--temp',
    help='In the case of multiple temperature channels, the channel may be '
        'chosen by index')
ctd_sbe_to_ctd_exchange_parser.add_argument(
    '-o', '--output',
    help='name of output file, _ct1.[csv, zip] will be added automatically, '
        'if not speified will default to standard out.')


def ctd_netcdf_to_ctd_netcdf_oceansites(args):
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.ctd.netcdf as ctdnc
    import libcchdo.formats.ctd.netcdf_oceansites as ctdnc_oceansites

    df = DataFile()

    with closing(args.ctdnc) as in_file:
        ctdnc.read(df, in_file)

    _qualify_oceansites_type()

    with closing(args.ctdnc_os) as out_file:
        ctdnc_oceansites.write(
            df, out_file, timeseries=args.timeseries, version=args.os_version)


ctd_netcdf_to_ctd_netcdf_oceansites_parser = ctd_converter_parsers.add_parser(
    'netcdf_to_netcdf_oceansites',
    help=ctd_netcdf_to_ctd_netcdf_oceansites.__doc__)
ctd_netcdf_to_ctd_netcdf_oceansites_parser.set_defaults(
    main=ctd_netcdf_to_ctd_netcdf_oceansites)
ctd_netcdf_to_ctd_netcdf_oceansites_parser.add_argument(
    '--os-version', choices=OCEANSITES_VERSIONS,
    default=OCEANSITES_VERSIONS[-1],
    help='OceanSITES version number (default: {0})'.format(
        OCEANSITES_VERSIONS[-1]))
ctd_netcdf_to_ctd_netcdf_oceansites_parser.add_argument(
    'ctdnc', type=argparse.FileType('r'),
    help='input CTD Exchange file')
ctd_netcdf_to_ctd_netcdf_oceansites_parser.add_argument(
    'ctdnc_os', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output CTD NetCDF OceanSITES file')
ctd_netcdf_to_ctd_netcdf_oceansites_parser.add_argument(
    'timeseries', type=str, nargs='?', default=None,
    choices=OCEANSITES_TIMESERIES,
    help='timeseries location (default: None)')


def ctdzip_andrex_to_ctdzip_exchange(args):
    from libcchdo.model.datafile import DataFileCollection
    from libcchdo.formats.ctd.zip import exchange as ctdzipex
    from libcchdo.formats.ctd.zip import netcdf_andrex as ctdzipnc_andrex

    dfc = DataFileCollection()

    with closing(args.ctdzip_andrex) as in_file:
        ctdzipnc_andrex.read(dfc, in_file)
    
    with closing(args.ctdzipex) as out_file:
        ctdzipex.write(dfc, out_file)


ctdzip_andrex_to_ctdzip_exchange_parser = ctd_converter_parsers.add_parser(
    'zip_andrex_to_zip_exchange',
    help=ctdzip_andrex_to_ctdzip_exchange.__doc__)
ctdzip_andrex_to_ctdzip_exchange_parser.set_defaults(
    main=ctdzip_andrex_to_ctdzip_exchange)
ctdzip_andrex_to_ctdzip_exchange_parser.add_argument(
    'ctdzip_andrex', type=argparse.FileType('r'),
    help='ANDREX NetCDF tar.gz')
ctdzip_andrex_to_ctdzip_exchange_parser.add_argument(
    'ctdzipex', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output CTD ZIP Exchange file')


def ctdzip_exchange_to_ctdzip_netcdf(args):
    from libcchdo.model.datafile import DataFileCollection
    import libcchdo.formats.ctd.zip.exchange as ctdzipex
    import libcchdo.formats.ctd.zip.netcdf as ctdzipnc

    dfc = DataFileCollection()
    LOG.debug(repr(args))

    with closing(args.ctdzipex) as in_file:
        ctdzipex.read(dfc, in_file)

    with closing(args.ctdzipnc) as out_file:
        ctdzipnc.write(dfc, out_file)


ctdzip_exchange_to_ctdzip_netcdf_parser = ctd_converter_parsers.add_parser(
    'zip_exchange_to_zip_netcdf',
    help=ctdzip_exchange_to_ctdzip_netcdf.__doc__)
ctdzip_exchange_to_ctdzip_netcdf_parser.set_defaults(
    main=ctdzip_exchange_to_ctdzip_netcdf)
ctdzip_exchange_to_ctdzip_netcdf_parser.add_argument(
    'ctdzipex', type=argparse.FileType('r'),
    help='input CTD ZIP Exchange file')
ctdzip_exchange_to_ctdzip_netcdf_parser.add_argument(
    'ctdzipnc', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output CTD ZIP NetCDF file')


def ctdzip_exchange_to_ctdzip_netcdf_oceansites(args):
    from libcchdo.model.datafile import DataFileCollection
    import libcchdo.formats.ctd.zip.exchange as ctdzipex
    import libcchdo.formats.ctd.zip.netcdf_oceansites as ctdzipnc_oceansites

    dfc = DataFileCollection()
    with closing(args.ctdzipex) as in_file:
        ctdzipex.read(dfc, in_file)
    
    _qualify_oceansites_type()

    with closing(args.ctdzipnc_os) as out_file:
        ctdzipnc_oceansites.write(
            dfc, out_file, timeseries=args.timeseries, version=args.os_version)


ctdzip_exchange_to_ctdzip_netcdf_oceansites_parser = \
    ctd_converter_parsers.add_parser(
        'zip_exchange_to_zip_netcdf_oceansites',
        help=ctdzip_exchange_to_ctdzip_netcdf_oceansites.__doc__)
ctdzip_exchange_to_ctdzip_netcdf_oceansites_parser.set_defaults(
    main=ctdzip_exchange_to_ctdzip_netcdf_oceansites)
ctdzip_exchange_to_ctdzip_netcdf_oceansites_parser.add_argument(
    '--os-version', choices=OCEANSITES_VERSIONS,
    default=OCEANSITES_VERSIONS[-1],
    help='OceanSITES version number (default: {0})'.format(
        OCEANSITES_VERSIONS[-1]))
ctdzip_exchange_to_ctdzip_netcdf_oceansites_parser.add_argument(
    'ctdzipex', type=argparse.FileType('r'),
    help='input CTD ZIP Exchange file')
ctdzip_exchange_to_ctdzip_netcdf_oceansites_parser.add_argument(
    'timeseries', type=str, nargs='?', default=None,
    choices=OCEANSITES_TIMESERIES,
    help='timeseries location (default: None)')
ctdzip_exchange_to_ctdzip_netcdf_oceansites_parser.add_argument(
    'ctdzipnc_os', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output CTD ZIP NetCDF OceanSITES file')


def ctdzip_netcdf_to_ctdzip_netcdf_oceansites(args):
    from libcchdo.model.datafile import DataFileCollection
    import libcchdo.formats.ctd.zip.netcdf as ctdzipnc
    import libcchdo.formats.ctd.zip.netcdf_oceansites as ctdzipnc_oceansites

    dfc = DataFileCollection()
    with closing(args.ctdzipnc) as in_file:
        ctdzipnc.read(dfc, in_file)
    
    _qualify_oceansites_type()

    with closing(args.ctdzipnc_os) as out_file:
        ctdzipnc_oceansites.write(
            dfc, out_file, timeseries=args.timeseries, version=args.os_version)


ctdzip_netcdf_to_ctdzip_netcdf_oceansites_parser = \
    ctd_converter_parsers.add_parser(
        'zip_netcdf_to_zip_netcdf_oceansites',
        help=ctdzip_netcdf_to_ctdzip_netcdf_oceansites.__doc__)
ctdzip_netcdf_to_ctdzip_netcdf_oceansites_parser.set_defaults(
    main=ctdzip_netcdf_to_ctdzip_netcdf_oceansites)
ctdzip_netcdf_to_ctdzip_netcdf_oceansites_parser.add_argument(
    '--os-version', choices=OCEANSITES_VERSIONS,
    default=OCEANSITES_VERSIONS[-1],
    help='OceanSITES version number (default: {0})'.format(
        OCEANSITES_VERSIONS[-1]))
ctdzip_netcdf_to_ctdzip_netcdf_oceansites_parser.add_argument(
    'ctdzipnc', type=argparse.FileType('r'),
    help='input CTD ZIP NetCDF file')
ctdzip_netcdf_to_ctdzip_netcdf_oceansites_parser.add_argument(
    'timeseries', type=str, nargs='?', default=None,
    choices=OCEANSITES_TIMESERIES,
    help='timeseries location (default: None)')
ctdzip_netcdf_to_ctdzip_netcdf_oceansites_parser.add_argument(
    'ctdzipnc_os', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output CTD ZIP NetCDF OceanSITES file')


def ctdzip_woce_and_summary_woce_to_ctdzip_exchange(args):
    from libcchdo.model.datafile import DataFileCollection, SummaryFile
    import libcchdo.formats.woce as woce
    import libcchdo.formats.summary.woce as sumwoce
    import libcchdo.formats.ctd.zip.woce as ctdzipwoce
    import libcchdo.formats.ctd.zip.exchange as ctdzipex

    ctdfiles = DataFileCollection()
    sumfile = SummaryFile()

    with closing(args.ctdzipwoce) as in_file:
        ctdzipwoce.read(ctdfiles, in_file)

    with closing(args.sumwoce) as in_file:
        sumwoce.read(sumfile, in_file)

    for ctdfile in ctdfiles.files:
        woce.combine(ctdfile, sumfile)

    with closing(args.ctdzipex) as out_file:
        ctdzipex.write(ctdfiles, out_file)


ctdzip_woce_and_summary_woce_to_ctdzip_exchange_parser = \
    ctd_converter_parsers.add_parser(
        'zip_woce_and_summary_woce_to_zip_exchange',
        help=ctdzip_woce_and_summary_woce_to_ctdzip_exchange.__doc__)
ctdzip_woce_and_summary_woce_to_ctdzip_exchange_parser.set_defaults(
    main=ctdzip_woce_and_summary_woce_to_ctdzip_exchange)
ctdzip_woce_and_summary_woce_to_ctdzip_exchange_parser.add_argument(
    'ctdzipwoce', type=argparse.FileType('r'),
    help='input CTD ZIP WOCE file')
ctdzip_woce_and_summary_woce_to_ctdzip_exchange_parser.add_argument(
    'sumwoce', type=argparse.FileType('r'),
    help='input Summary WOCE file')
ctdzip_woce_and_summary_woce_to_ctdzip_exchange_parser.add_argument(
    'ctdzipex', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output CTD ZIP Exchange file')


sum_converter_parser = converter_parsers.add_parser(
    'summary',
    help='Summary file converters')
sum_converter_parsers = sum_converter_parser.add_subparsers(
    title='Summary file converters')


def summary_hot_to_summary_woce(args):
    """Convert HOT program summary file to WOCE format summary file."""
    from libcchdo.model.datafile import SummaryFile
    import libcchdo.formats.summary.hot as sumhot
    import libcchdo.formats.summary.woce as sumwoce

    sf = SummaryFile()

    with closing(args.input_sumhot) as in_file:
        sumhot.read(sf, in_file)

    with closing(args.output_sumwoce) as out_file:
        sumwoce.write(sf, out_file)


summary_hot_to_summary_woce_parser = sum_converter_parsers.add_parser(
    'hot_to_woce',
    help=summary_hot_to_summary_woce.__doc__)
summary_hot_to_summary_woce_parser.set_defaults(
    main=summary_hot_to_summary_woce)
summary_hot_to_summary_woce_parser.add_argument(
    'input_sumhot', type=argparse.FileType('r'),
    help='input Summary HOT file')
summary_hot_to_summary_woce_parser.add_argument(
    'output_sumwoce', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output Summary WOCE file (default: stdout)')


to_kml_converter_parser = converter_parsers.add_parser(
    'to_kml',
    help='Convert to KML')
to_kml_converter_parsers = to_kml_converter_parser.add_subparsers(
    title='Convert to KML')


def db_to_kml(args):
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.bottle.exchange as botex
    from libcchdo.kml import db_to_kml, db_to_kml_full

    df = DataFile()
    
    with closing(args.input_botex) as in_file:
        botex.read(df, in_file)

    with closing(args.output) as out_file:
        if args.full:
            db_to_kml_full(df, out_file)
        else:
            db_to_kml(df, out_file)


db_to_kml_parser = to_kml_converter_parsers.add_parser(
    'db',
    help=db_to_kml.__doc__)
db_to_kml_parser.set_defaults(
    main=db_to_kml)
db_to_kml_parser.add_argument(
    'input_botex', type=argparse.FileType('r'),
    help='input Bottle Exchange file')
db_to_kml_parser.add_argument(
    '--full', type=bool, default=False,
    help='full with dates')
db_to_kml_parser.add_argument(
    'output', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output file (default: stdout)')


def db_track_lines_to_kml(args):
    from libcchdo.kml import db_track_lines_to_kml

    db_track_lines_to_kml_parser()


db_track_lines_to_kml_parser = to_kml_converter_parsers.add_parser(
    'db_track_lines',
    help=db_track_lines_to_kml.__doc__)
db_track_lines_to_kml_parser.set_defaults(
    main=db_track_lines_to_kml)


misc_converter_parser = converter_parsers.add_parser(
    'misc',
    help='Miscellaneous converters')
misc_converter_parsers = misc_converter_parser.add_subparsers(
    title='Miscellaneous converters')


def convert_per_litre_to_per_kg_botex(args):
    from libcchdo.tools import convert_per_litre_to_per_kg

    df = DataFile()

    with closing(args.input_botex) as in_file:
        botex.read(df, in_file)

    convert_per_litre_to_per_kg(df)

    with closing(args.output_botex) as out_file:
        botex.write(file, f)


convert_per_litre_to_per_kg_botex_parser = \
    misc_converter_parsers.add_parser(
    'per_litre_to_per_kg_botex',
    help=convert_per_litre_to_per_kg_botex.__doc__)
convert_per_litre_to_per_kg_botex_parser.set_defaults(
    main=convert_per_litre_to_per_kg_botex)
convert_per_litre_to_per_kg_botex_parser.add_argument(
    'input_botex', type=argparse.FileType('r'),
    help='input Bottle Exchange file')
convert_per_litre_to_per_kg_botex_parser.add_argument(
    'output_botex', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output Bottle Exchange file')


def convert_hly0301(args):
    """Make changes specific to HLY0301 by request from D. Muus."""
    from libcchdo.model.datafile import DataFileCollection
    from libcchdo.formats.ctd.zip import exchange as ctdzipex
    from libcchdo.tools import operate_healy_file

    dfc = DataFileCollection()

    with closing(args.input_ctdzipex) as in_file:
        ctdzipex.read(dfc, in_file, retain_order=True)

    for f in dfc.files:
        operate_healy_file(f)

    with closing(args.out_ctdzipex) as out_file:
        ctdzipex.write(dfc, out_file)


convert_hly0301_parser = misc_converter_parsers.add_parser(
    'hly0301',
    help=convert_hly0301.__doc__)
convert_hly0301_parser.set_defaults(
    main=convert_hly0301)


merge_parser = hydro_subparsers.add_parser(
    'merge', help='Mergers')
merge_parsers = merge_parser.add_subparsers(title='mergers')


def merge_ctd_bacp_xmiss_and_ctd_exchange(args):
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.ctd.bacp as ctdbacp
    import libcchdo.formats.ctd.exchange as ctdex

    mergefile = DataFile()
    df = DataFile()

    with closing(args.ctd_bacp) as in_file:
        ctdbacp.read(mergefile, in_file)
    with closing(args.in_ctdex) as in_file:
        ctdex.read(df, in_file)

    merge_ctd_bacp_xmiss_and_ctd_exchange_parser(mergefile, df)

    with closing(args.out_ctdex) as out_file:
        ctdex.write(df, out_file)


merge_ctd_bacp_xmiss_and_ctd_exchange_parser = merge_parsers.add_parser(
    'ctd_bacp_xmiss_and_ctd_exchange',
    help=merge_ctd_bacp_xmiss_and_ctd_exchange.__doc__)
merge_ctd_bacp_xmiss_and_ctd_exchange_parser.set_defaults(
    main=merge_ctd_bacp_xmiss_and_ctd_exchange)
merge_ctd_bacp_xmiss_and_ctd_exchange_parser.add_argument(
    'ctd_bacp', type=argparse.FileType('r'),
    help='input CTD BACP file')
merge_ctd_bacp_xmiss_and_ctd_exchange_parser.add_argument(
    'in_ctdex', type=argparse.FileType('r'),
    help='input CTD Exchange file')
merge_ctd_bacp_xmiss_and_ctd_exchange_parser.add_argument(
    'out_ctdex', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output CTD Exchange file')


def merge_botex_and_botex(args):
    """Merge two Bottle Exchange files together.

    If no parameters to merge are given, show the parameters that have differing
    data.

    """
    from libcchdo.model.datafile import DataFile
    from libcchdo.merge import Merger, convert_to_datafile
    import libcchdo.formats.bottle.exchange as botex

    with closing(args.file1) as in_file1:
        with closing(args.file2) as in_file2:
            m = Merger(f1_handle, f2_handle)
            if args.parameters_to_merge:
                columns_to_merge = []
                units_to_merge = []
                for parameter in args.parameters_to_merge:
                    columns_to_merge.append(parameter)
                    unit_index = \
                        m.dataframe2.columns.values.tolist().index(parameter)
                    units_to_merge.append(m.units2[unit_index])
                df = DataFile()
                result_units = m.units1 + (units_to_merge)
                result = m.mergeit(columns_to_merge)
                convert_to_datafile(
                    df, m.header1, result, result_units, m.stamp1)
                with closing(args.output) as out_file:
                    botex.write(df, out_file)
            else:
                # Show parameters with differing data
                different_columns = m.different_cols()
                with closing(args.output) as out_file:
                    out_file.write(
                        u'The following parameters in {0} are different'
                        '\n'.format(file2))
                for col in different_columns:
                    out_file.write(u'{0}\n'.format(col))


merge_botex_and_botex_parser = merge_parsers.add_parser(
    'botex_and_botex',
    help=merge_botex_and_botex.__doc__)
merge_botex_and_botex_parser.set_defaults(
    main=merge_botex_and_botex)
merge_botex_and_botex_parser.add_argument(
    'file1', type=argparse.FileType('r'),
    help='first file to merge')
merge_botex_and_botex_parser.add_argument(
    'file2', type=argparse.FileType('r'),
    help='second file to merge')
merge_botex_and_botex_group_merge = \
    merge_botex_and_botex_parser.add_argument_group(
        title='Merge parameters')
merge_botex_and_botex_group_merge.add_argument(
    '--output', type=argparse.FileType('w'), nargs='+', default=sys.stdout,
    help='output Bottle Exchange file')
merge_botex_and_botex_group_merge.add_argument(
    'parameters_to_merge', type=str, nargs='*', default=[],
    help='parameters to merge')


datadir_parser = hydro_subparsers.add_parser(
    'datadir', help='CCHDO data directory utilities')
datadir_parsers = datadir_parser.add_subparsers(
    title='CCHDO data directory utilities')


def datadir_ensure_navs(args):
    from libcchdo.datadir.util import do_for_cruise_directories
    from libcchdo.tools import ensure_navs

    do_for_cruise_directories(ensure_navs)


datadir_ensure_navs_parser = datadir_parsers.add_parser(
    'ensure_navs',
    help=datadir_ensure_navs.__doc__)
datadir_ensure_navs_parser.set_defaults(
    main=datadir_ensure_navs)


def datadir_mkdir_working(args):
    """Create a working directory for data work.

    """
    from libcchdo.datadir.util import mkdir_ensure, make_subdirs
    date = datetime.strptime(args.date, '%Y-%m-%d').date()
    dirname = args.separator.join(
        [date.strftime('%Y.%m.%d'), args.title, args.person])
    dirpath = os.path.join(args.basepath, dirname)

    dir_perms = 0770
    file_perms = 0660

    mkdir_ensure(dirpath, dir_perms)

    files = ['00_README.txt']
    subdirs = [
        'submission',
        ['processing', ['exchange', 'woce', 'netcdf']],
        'to_go_online',
    ]

    for fname in files:
        fpath = os.path.join(dirpath, fname)
        try:
            os.chmod(fpath, file_perms)
        except OSError:
            pass
        with file(fpath, 'a'):
            os.utime(fpath, None)
            os.chmod(fpath, file_perms)
    make_subdirs(dirpath, subdirs, dir_perms)

    print dirpath


datadir_mkdir_working_parser = datadir_parsers.add_parser(
    'mkdir_working',
    help=datadir_mkdir_working.__doc__)
datadir_mkdir_working_parser.set_defaults(
    main=datadir_mkdir_working)
datadir_mkdir_working_parser.add_argument(
    '--basepath', default=os.getcwd(),
    help='Base path to put working directory in (default: current directory)')
datadir_mkdir_working_parser.add_argument(
    '--separator', default='_')
datadir_mkdir_working_parser.add_argument(
    '--date', default=date.today().isoformat(),
    help='The date for the work being done (default: today)')
datadir_mkdir_working_parser.add_argument(
    '--title', default='working',
    help='A title for the work being done. E.g. CTD, BOT, params '
        '(default: working)')
datadir_mkdir_working_parser.add_argument(
    '--person', default=os.getlogin(),
    help='The person doing the work (default: {0})'.format(os.getlogin()))


def datadir_copy_replaced(args):
    """Move a replaced file to its special name.

    """
    import shutil
    from libcchdo.fns import file_extensions, guess_file_type

    dirname, filename = os.path.split(args.filename)
    dirname = os.path.join(os.getcwd(), dirname)
    file_type = guess_file_type(filename)
    if file_type is None:
        LOG.error(
            u'File {0} does not have a recognizable file extension.'.format(
            args.filename))
        return 1

    exts = file_extensions[file_type]
    sorted_exts = sorted(
        zip(exts, map(len, exts)), key=lambda x: x[1], reverse=True)
    exts = [x[0] for x in sorted_exts]

    basename = args.filename
    extension = None
    for ext in exts:
        if args.filename.endswith(ext):
            basename = args.filename[:-len(ext)]
            extension = ext

    date = datetime.strptime(args.date, '%Y-%m-%d').date()
    replaced_str = args.separator.join(
        ['', 'rplcd', date.strftime('%Y%m%d'), ''])
    extra_extension = extension.split('.')[0]

    new_name = os.path.relpath(os.path.join(dirname, 'original', ''.join(
        [basename, extra_extension, replaced_str, extension])))

    print args.filename, '->', new_name
    accepted = raw_input('copy? (y/[n]) ')
    if accepted == 'y':
        try:
            shutil.copy2(args.filename, new_name)
        except OSError, e:
            LOG.error(u'Could not move file: {0}'.format(e))
            return 1


datadir_copy_replaced_parser = datadir_parsers.add_parser(
    'copy_replaced',
    help=datadir_copy_replaced.__doc__)
datadir_copy_replaced_parser.set_defaults(
    main=datadir_copy_replaced)
datadir_copy_replaced_parser.add_argument(
    '--separator', default='_')
datadir_copy_replaced_parser.add_argument(
    '--date', default=date.today().isoformat(),
    help='The date for the work being done (default: today)')
datadir_copy_replaced_parser.add_argument(
    'filename', 
    help='The file that is replaced and needs to be moved.')


plot_parser = hydro_subparsers.add_parser(
    'plot', help='Plotters')
plot_parsers = plot_parser.add_subparsers(title='plotters')


def plot_etopo(args):
    """Plot the world with ETOPO bathymetry."""
    from libcchdo.fns import read_arbitrary
    from libcchdo.tools import plot_etopo

    bm = plot_etopo(args)
    if args.any_file:
        df = read_arbitrary(args.any_file)

        lats = df['LATITUDE']
        lons = df['LONGITUDE']
        if not (lats and lons):
            LOG.error(u'Cannot plot file without LATITUDE and LONGITUDE data')
            return
        xs, ys = bm(lons.values, lats.values)

        dots = bm.scatter(xs, ys, **bm.GMT_STYLE_DOTS)
        line = bm.plot(xs, ys, **bm.GMT_STYLE_LINE)

    bm.savefig(args.output_filename)


plot_etopo_parser = plot_parsers.add_parser(
    'etopo',
    help=plot_etopo.__doc__)
plot_etopo_parser.set_defaults(
    main=plot_etopo)
plot_etopo_parser.add_argument(
    'minutes', type=int, nargs='?', default=5, choices=[1, 2, 5, 30, 60], 
    help='The desired resolution of the ETOPO grid data in minutes '
         '(default: 5)')
plot_etopo_parser.add_argument(
    '--width', type=int, default=720, choices=[240, 320, 480, 720, 1024],
    help='The desired width in pixels of the resulting plot image '
         '(default: 720)')
plot_etopo_parser.add_argument(
    '--fill_continents', type=bool, default=False,
    help='Whether to fill the continent interiors with solid black '
         '(default: False)')
plot_etopo_parser.add_argument(
    '--projection', default='merc',
    choices=['merc', 'robin', 'npstere', 'spstere', ],
    help='The projection of map to use (default: merc)')
plot_etopo_parser.add_argument(
    '--cmap', default='cberys',
    choices=['cberys', 'gray'],
    help='The colormap to use for the ETOPO data (default: cberys)')
plot_etopo_parser.add_argument(
    '--title', type=str, 
    help='A title for the plot')
plot_etopo_parser.add_argument(
    '--any-file', type=argparse.FileType('r'), nargs='?',
    help='Name of an input file to plot points for')
plot_etopo_parser.add_argument(
    '--output-filename', default='etopo.png',
    help='Name of the output file (default: etopo.png)')
_llcrnrlat = -89
# Chosen so that the date line will be centered
_llcrnrlon = 25
plot_etopo_parser.add_argument(
    '--bounds-cylindrical', type=float, nargs=4,
    default=[_llcrnrlon, _llcrnrlat, 360 + _llcrnrlon, -_llcrnrlat],
    help='The boundaries of the map as '
         '[llcrnrlon, llcrnrlat, urcrnrlon, urcrnrlat]')
# TODO these options need to be matched with the projection
plot_etopo_parser.add_argument(
    '--bounds-elliptical', type=float, nargs=1,
    default=180,
    help='The center meridian of the map lon_0 (default: 180 centers the '
        'Pacific Ocean)')

misc_parser = hydro_subparsers.add_parser(
    'misc', help='Miscellaneous')
misc_parsers = misc_parser.add_subparsers(title='miscellaneous')


def any_to_legacy_parameter_statuses(args):
    """Show legacy parameter ids for the parameters in a data file."""
    from libcchdo.fns import read_arbitrary
    from libcchdo.tools import df_to_legacy_parameter_statuses

    with closing(args.input_file) as in_file:
        data = read_arbitrary(in_file, args.input_type)
    
    with closing(args.output_file) as out_file:
        df_to_legacy_parameter_statuses(data, out_file)


any_to_legacy_parameter_statuses_parser = misc_parsers.add_parser(
    'any_to_legacy_parameter_statuses',
    help=any_to_legacy_parameter_statuses.__doc__)
any_to_legacy_parameter_statuses_parser.set_defaults(
    main=any_to_legacy_parameter_statuses)
any_to_legacy_parameter_statuses_parser.add_argument('-i', '--input-type',
    choices=known_formats,
    help='force the input file to be read as the specified type')
any_to_legacy_parameter_statuses_parser.add_argument(
    'input_file', type=argparse.FileType('r'),
    help='any recognized CCHDO file')
any_to_legacy_parameter_statuses_parser.add_argument(
    'output_file', type=argparse.FileType('w'), nargs='?',
    default=sys.stdout,
    help='output file (default: stdout)')


def bottle_exchange_canon(args):
    """Rewrite a bottle exchange file with all parameters converted to canon.

    """
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.bottle.exchange as botex

    df = DataFile(allow_contrived=True)

    with closing(args.input_botex) as in_file:
        botex.read(df, in_file)

    with closing(args.output_botex) as out_file:
        botex.write(df, out_file)


bottle_exchange_canon_parser = misc_parsers.add_parser(
    'bottle_exchange_canon',
    help=bottle_exchange_canon.__doc__)
bottle_exchange_canon_parser.set_defaults(
    main=bottle_exchange_canon)
bottle_exchange_canon_parser.add_argument(
    'input_botex', type=argparse.FileType('r'),
    help='input Bottle Exchange file')
bottle_exchange_canon_parser.add_argument(
    'output_botex', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output Bottle Exchange file (default: stdout)')


def collect_into_archive(args):
    from libcchdo.tools import collect_into_archive

    collect_into_archive()


collect_into_archive_parser = misc_parsers.add_parser(
    'collect_into_archive',
    help=collect_into_archive.__doc__)
collect_into_archive_parser.set_defaults(
    main=collect_into_archive)


def rebuild_hot_bats_oceansites(args):
    from libcchdo.datadir.util import do_for_cruise_directories
    from libcchdo.tools import rebuild_hot_bats_oceansites

    do_for_cruise_directories(rebuild_hot_bats_oceansites)


rebuild_hot_bats_oceansites_parser = misc_parsers.add_parser(
    'rebuild_hot_bats_oceansites',
    help=rebuild_hot_bats_oceansites.__doc__)
rebuild_hot_bats_oceansites_parser.set_defaults(
    main=rebuild_hot_bats_oceansites)


def reorder_surface_to_bottom(args):
    """Orders the data rows in a bottle file by pressure and bottle number

    Defaults to non-descending pressure, non-ascending bottle number order.

    """
    from libcchdo.model.datafile import DataFile
    import libcchdo.formats.bottle.exchange as botex

    df = DataFile()

    with closing(args.input_file) as f:
        botex.read(df, f)

    df.reorder_file_pressure(
        args.order_nondesc_pressure, args.order_nondesc_btlnbr)

    with closing(args.output_file) as f:
        botex.write(df, f)


reorder_surface_to_bottom_parser = misc_parsers.add_parser(
    'reorder_surface_to_bottom',
    #aliases=['reorder'],
    help=reorder_surface_to_bottom.__doc__)
reorder_surface_to_bottom_parser.set_defaults(
    main=reorder_surface_to_bottom)
reorder_surface_to_bottom_parser.add_argument(
    'input_file', type=argparse.FileType('r'),
    help='input Bottle Exchange file')
reorder_surface_to_bottom_parser.add_argument(
    'output_file', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output Bottle Exchange file')
reorder_surface_to_bottom_parser.add_argument(
    '--order-nondesc-pressure', type=bool, nargs='?', default=True,
    help='Order by non-descending pressure (default: True)')
reorder_surface_to_bottom_parser.add_argument(
    '--order-nondesc-btlnbr', type=bool, nargs='?', default=False,
    help='Order by non-descending bottle number (default: False)')


report_parser = hydro_subparsers.add_parser(
    'report', help='Reports')
report_parsers = report_parser.add_subparsers(title='Reports')


def report_data_updates(args):
    """Generate report of number of data formats change in a time range.

    Defaults to the past fiscal year.

    """
    from libcchdo.reports import report_data_updates

    report_data_updates(args)


today = datetime.utcnow()


report_data_updates_parser = report_parsers.add_parser(
    'data_updates',
    help=report_data_updates.__doc__)
report_data_updates_parser.set_defaults(
    main=report_data_updates)
report_data_updates_parser.add_argument(
    '--year', nargs='?', default=today.year,
    help='Year to end')
report_data_updates_parser.add_argument(
    '--month', nargs='?', default=today.month,
    help='Month to end')
report_data_updates_parser.add_argument(
    '--day', nargs='?', default=today.day,
    help='Day to end')
report_data_updates_parser.add_argument(
    'output', type=argparse.FileType('w'), nargs='?', default=sys.stdout,
    help='output file')


def deprecated_reorder_surface_to_bottom():
    call_deprecated('misc', 'reorder_surface_to_bottom')


def _subparsers(parser):
    try:
        return parser._subparsers._group_actions[0]._name_parser_map.values()
    except AttributeError:
        return []


def _rewrite_subparser_prog(parser, full_prog):
    subparsers = _subparsers(parser)
    for subparser in subparsers:
        subparser.prog = full_prog
        _rewrite_subparser_prog(subparser, full_prog)
    

def call_deprecated(*subcmds):
    """Notify that executable is deprecated and to use the main program."""
    prog = 'hydro'
    subcmds = list(subcmds)
    full_prog = u' '.join([prog] + subcmds)
    hydro_parser.prog = prog
    hydro_parser._prog = prog

    # Rewrite subparser prog so usage print is correct
    _rewrite_subparser_prog(hydro_parser, full_prog)

    LOG.error(
        u'DEPRECATED: Please use {0}.'.format(full_prog))
    sys.argv = [prog] + subcmds + sys.argv[1:]
    main()
    

def main():
    """The main program that wraps all subcommands."""
    args = hydro_parser.parse_args()
    hydro_parser.exit(args.main(args))
