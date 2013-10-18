from copy import copy
import code
import struct
import sys
import os
import os.path
import shutil
from tarfile import TarFile
from contextlib import closing
from csv import reader as csv_reader
from collections import OrderedDict
import readline
import atexit

import filecmp

from tempfile import mkdtemp

import numpy as np

from sqlalchemy.sql import or_, distinct

from libcchdo import config
from libcchdo.util import StringIO
from libcchdo.log import LOG
from libcchdo.db import connect
from libcchdo.db.util import wkt_to_track, tracks_for_cruises
from libcchdo.db.model import legacy, std
from libcchdo.db.model.legacy import Document
from libcchdo.model.datafile import DataFile, DataFileCollection, Column
from libcchdo.units import convert as ucvt
from libcchdo.formats import add_pre_write
from libcchdo.fns import equal_with_epsilon
from libcchdo.formats.formats import read_arbitrary
from libcchdo.formats.ctd import asc
from libcchdo.formats import zip as Zip
from libcchdo.formats.zip import ZeroCommentZipFile, MemZipFile
import libcchdo.formats.summary.woce as sumwoce
import libcchdo.formats.bottle.exchange as botex
import libcchdo.formats.ctd.exchange as ctdex
import libcchdo.formats.ctd.sbe9 as sbe
import libcchdo.formats.ctd.zip.netcdf_oceansites as ctdzipnc_os
import libcchdo.formats.ctd.zip.exchange as ctdzipex
from libcchdo.plot.etopo import plot, plot_line_dots, plt, ETOPOBasemap
from libcchdo.plot import presets_goship, gmt_color


def _get_legacy_parameter(session, parameter):
    if not parameter:
        return None
    name = parameter.name
    if name != '_DATETIME':
        return legacy.find_parameter(session, name)
    return None


def df_to_legacy_parameter_statuses(self, output):
    # Get STD parameters from DataFile
    parameters = self.get_property_for_columns(lambda x: x.parameter)

    LOG.info(u'Parameters for the data set are: {0!r}\n'.format(parameters))

    legacy_session = legacy.session()
    legacy_parameters = filter(
        None,
        [_get_legacy_parameter(legacy_session, x) for x in parameters])
    legacy_session.close()

    id_names = [
        (int(x.id) if x.id else None, x.name) for x in legacy_parameters]
    output.write(u'{0!r}\n'.format(id_names))


def _clean_for_filename(x):
    return x.replace('/', '_').replace('.', '_')


def collect_into_archive():
    session = connect.session(connect.cchdo())

    # let's go and get all the directories.
    ftypes = ['Bottle', 'Exchange Bottle', 'Exchange Bottle (Zipped)']
    bot_query = or_(*[Document.FileType == x for x in ftypes])
    doc_types = ['Documentation', 'PDF Documentation']
    bot_doc_query = or_(*[Document.FileType == x for x in ftypes + doc_types])
    expocodes_with_bottle = session.query(distinct(Document.ExpoCode)).filter(
        bot_query).all()
    expocodes_with_bottle = [x[0] for x in expocodes_with_bottle]
    expocodes_with_bottle.remove(None)
    expocodes_with_bottle.remove('NULL')

    tempdir = mkdtemp()
    LOG.debug(tempdir)

    # Get all required files for the cruises.
    for expocode in expocodes_with_bottle:
        docs = session.query(Document).filter(
            Document.ExpoCode == expocode).filter(bot_doc_query).all()
        cruise_dir = os.path.join(tempdir, _clean_for_filename(expocode))
        os.makedirs(cruise_dir)

        #LOG.debug(expocode)
        for doc in docs:
            datapath = doc.FileName
            tmppath = os.path.join(cruise_dir, os.path.basename(datapath))

            try:
                shutil.copy(datapath, tmppath)
            except IOError:
                LOG.warn(u'missing file: {}'.format(datapath))
    session.close()

    #for root, dirs, files in os.walk(path):
    #    for momo in dirs:
    #        os.chown(os.path.join(root, momo), 502, 20)
    #    for momo in files:
    #os.chown(os.path.join(root, momo), 502, 20)

    cwd = os.getcwd()
    os.chdir(tempdir)

    tarball = TarFile(mode='w', fileobj=sys.stdout)
    tarball.add('.')
    tarball.close()

    os.chdir(cwd)

    shutil.rmtree(tempdir)


class HistoryConsole(code.InteractiveConsole):
    def __init__(self, locals=None, filename="<console>",
                 histfile=os.path.expanduser('~/.libcchdo/.console-history')):
        code.InteractiveConsole.__init__(self, locals, filename)
        self.init_history(histfile)

    def init_history(self, histfile):
        readline.parse_and_bind('tab: complete')
        if hasattr(readline, 'read_history_file'):
            try:
                readline.read_history_file(histfile)
            except IOError:
                pass
            atexit.register(self.save_history, histfile)

    def save_history(self, histfile):
        readline.write_history_file(histfile)



def _check_and_replace_parameters_convert(self, default_convert=False):
    for column in self.columns.values():
        parameter = column.parameter
        std_parameter = std.find_by_mnemonic(parameter.name)

        if not std_parameter and not parameter.name.startswith('_'):
            LOG.warn("Unknown parameter '%s'" % parameter.name)
            continue

        given_units = parameter.units.mnemonic if parameter.units else None
        expected_units = std_parameter.units.mnemonic \
            if std_parameter.units else None
        from_to = (given_units, expected_units)

        if given_units and expected_units and given_units != expected_units:
            LOG.warn(("Mismatched units for '%s'. "
                      "Found '%s' but expected '%s'") % \
                      ((parameter.name,) + from_to))
            try:
                unit_converter = self.unit_converters[from_to]
                convert = None
                if default_convert:
                    convert = 'y'
                while not convert or convert.lower() not in ('y', 'n'):
                    try:
                        convert = raw_input(
                            ("Convert from '%s' to '%s' for '%s'? (y/n)") % \
                            (from_to + (parameter.name,)))
                    except EOFError:
                        pass
                if convert == 'y':
                    LOG.info("Converting from '%s' -> '%s' for %s." % \
                             (from_to + (column.parameter.name,)))
                    column = unit_converter(self, column)
                else:
                    # Skip conversion and unit change.
                    continue
            except KeyError:
                LOG.info(("No unit converter registered with file for "
                          "'%s' -> '%s'. Skipping conversion.") % from_to)
                continue

        column.parameter = std_parameter


def convert_per_litre_to_per_kg(
        file, whole_not_aliquot=None, default_convert=False):
    """Convert WOCE format /L units to /KG.

    A small percentage of WOCE format hydro data is submitted with oxygens
    (both bottle and CTD) in ML/L and with nutrients in UMOL/L.  This program
    will detect and convert data in /L units and convert them to /KG units.
     
    Program asks for:    input filename   (.HYD or .SEA file)
                         output filename:
                         if bottle oxygens:
                               were oxygens whole bottle or aliquot 
     
    notes:
        Input format is assumed to be correctly formatted WHP data
        Data columns identifiers and units strings must be correct
          and in upper case.  ie.  oxygens are ML/L
        Oxygen conversion uses sigma T for density. T is set at
          25 C for aliquot oxygens and when T is missing.  

    """
    file.unit_converters[('DEG C', u'ITS-90')] = ucvt.equivalent

    file.unit_converters[('ML/L', u'UMOL/KG')] = \
        lambda f, c: ucvt.milliliter_per_liter_to_umol_per_kg(
            f, c, whole_not_aliquot)
    file.unit_converters[('UMOL/L', u'UMOL/KG')] = \
        ucvt.mol_per_liter_to_mol_per_kg
    file.unit_converters[('PMOL/L', u'PMOL/KG')] = \
        ucvt.mol_per_liter_to_mol_per_kg
    file.unit_converters[('NMOL/L', u'NMOL/KG')] = \
        ucvt.mol_per_liter_to_mol_per_kg
    # XXX YIKES but it's there in the fortran
    #file.unit_converters[('MMOL/L', u'UMOL/KG')] = \
    #    ucvt.mol_per_liter_to_mol_per_kg
    _check_and_replace_parameters_convert(file, default_convert)


def operate_healy_file(df):
    LOG.info('Attaching unit converters')
    cvt = ucvt.ctdoxy_micromole_per_liter_to_micromole_per_kilogram
    df.unit_converters[('UMOL/L', 'UMOL/KG')] = cvt
    df.unit_converters[('MMOLE/M^3', 'UMOL/KG')] = cvt
    df.unit_converter_technique[cvt] = '(MMOL/M^3)/(1 + CTDSIGTH/1000)'

    stamp = config.stamp()

    def change_stamp(self):
        self.globals['stamp'] = stamp

    def add_sect_id(self):
        self.globals['SECT_ID'] = 'CAA'

    add_pre_write(change_stamp)
    df.changes_to_report.append('Added SECT_ID = CAA')
    add_pre_write(add_sect_id)

    df.check_and_replace_parameters()

    # Remove columns
    LOG.info('Removing unwanted columns')
    unwanted = ('CTDDEP CTDPOTTMP CTDSIGTH CTDOXYV CTDOXPCSAT '
                'CTDOXSAT CTDNOBS').split()
    deleted = []
    for c in unwanted:
        try:
            del df[c]
            deleted.append(c)
        except KeyError:
            pass
    if deleted:
        df.changes_to_report.append('Removed columns: %s' % ', '.join(deleted))

    # Change expocode
    LOG.info('Changing expocode')
    expocode = '32H120030721'
    df.globals['EXPOCODE'] = expocode
    df.changes_to_report.append('Changed EXPOCODE from HLY031 to 32H120030721')

    # Add flag 2 to all columns
    LOG.info('Adding QC flags')
    for c in df.columns.values():
        c.flags_woce = [2] * len(c)


rebuild_roots = ['co2clivar/%s' % root for root in 
    ['atlantic/bats/ars01/%s' % base for base in [
        '33h420081015',
        '33h420090506',
        '33h420090514',
        '33h420090609',
    ]] + \
    ['pacific/hot/prs2/%s' % base for base in [
        'hot-062',
        'hot-074',
        'hot-075',
    ]]
]


def rebuild_hot_bats_oceansites(root, dirs, files):
    realedit = True

    if root not in rebuild_roots:
        return False

    if not root.startswith('co2clivar/atlantic/bats') and \
       not root.startswith('co2clivar/pacific/hot'):
        return False

    filename = None
    for file in files:
        if file.endswith('nc_ctd_oceansites.zip'):
            filename = file
            break

    if not filename:
        return

    if filename.startswith('placeholder'):
        if realedit:
            os.unlink(os.path.join(root, filename))
        else:
            LOG.info('would unlink %s', os.path.join(root, filename))

    ctdzipexs = filter(lambda f: f.endswith('ct1.zip'), files)
    if len(ctdzipexs) > 1:
        paths = [os.path.join(root, name) for name in ctdzipexs]
        mtimes = [os.lstat(path).st_mtime for path in paths]
        if len(set(mtimes)) == 1:
            if filecmp.cmp(*paths):
                ctdzipex_path = paths[0]
            else:
                LOG.debug('cant determine which ctdzipex to use')
                return
        else:
            # Use most recently modified ctdzipex
            ctdzipex_path = paths[mtimes.index(max(mtimes))]
    elif len(ctdzipexs) < 1:
        LOG.warn('has no ctdzipexs')
        return
    else:
        ctdzipex_path = os.path.join(root, ctdzipexs[0])

    with open(ctdzipex_path) as file:
        try:
            dfc = read_arbitrary(file, 'ctdzipex')
        except Exception, e:
            print >> sys.stderr, 'Failed to read ctdzipex format for %s' % root
            print >> sys.stderr, e
            return False

    if 'bats' in root:
        ts = 'BATS'
    elif 'hot' in root:
        ts = 'HOT'

    buff = StringIO()
    #try:
    ctdzipnc_os.write(dfc, buff, timeseries=ts)
    #except Exception, e:
    #    print >> sys.stderr, 'Failed to write oceansites format for %s' % root
    #    print >> sys.stderr, e
    #    del buff
    #    return False
    buff.flush()
    buff.seek(0)

    if realedit:
        ctdzipnc_os_path = ctdzipex_path.replace('ct1.zip', '') + \
                'nc_ctd_oceansites.zip.new'
        with open(ctdzipnc_os_path, 'w') as f:
            while True:
                data = buff.read(2<<16)
                if not data:
                    break
                f.write(data)
    else:
        print str(buff)

    buff.close()


def ensure_navs(root, dirs, files):
    navfiles = filter(lambda f: f.endswith('na.txt'), files)
    if len(navfiles) > 0:
        LOG.info('%s has nav files %s' % (root, ', '.join(navfiles)))
    else:
        LOG.info(("%s is missing a nav file. "
                      "Attempting to generate one.") % root)
    # Try to use easiest generation method first
    generation_methods = [
        ['Bottle Exchange', 'hy1.csv', botex.read],
        ['Summary', 'su.txt', sumwoce.read],
        # Other WOCE files do not have lng lat (they're in the Summary file)
        # TODO Collections have to have some regular way to be merged before
        # they can be outputted to nav.
        #['CTD Exchange', 'ct1.zip',
        #  DataFileCollection.read_CTDZip_Exchange],
        #['Bottle NetCDF', 'nc_hyd.zip',
        #  DataFileCollection.read_BottleZip_NetCDF],
        #['CTD NetCDF', 'nc_ctd.zip',
        #  DataFileCollection.read_CTDZip_NetCDF],
    ]
    for methodname, extension, readfn in generation_methods:
        basefiles = filter(lambda f: f.endswith(extension), files)
        if len(basefiles) > 0:
            LOG.info('  Found a %s file.' % methodname)
            for file in basefiles:
                try:
                    outputfile = '%sna.txt' % file[:-len(extension)]
                    LOG.info('  Generating nav file %s from a %s file %s.' % \
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
                    LOG.info(("Unable to generate. The read function has not been "
                              "implemented: %s") % e)
                except struct.error, e1:
                    LOG.info(("  Ignoring WOCE unpack error and continuing with "
                              "different method: %s") % e1)
                except NameError, e2:
                    if str(e2).endswith("not in CCHDO's parameter list."):
                        LOG.info('  Ignoring parameter not in database error.')
                    else:
                        LOG.warning('  Ignoring exception: %s' % e2)
                except ValueError, e3:
                    if str(e3).startswith("time data did not match format"):
                        LOG.info('  Ignoring time data format error: %s' % e3)
                    else:
                        LOG.warning('  Ignoring exception: %s' % e3)
                except Exception, ee:
                    LOG.warning('  Ignoring exception: %s' % ee)
        LOG.info('  Unable to find a %s file.' % methodname)
    LOG.warning('  Unable to generate a nav file for %s' % root)
    return False


def ctd_polarstern_to_ctd_exchange(args, db):
    import libcchdo.formats.ctd.exchange as ctdex
    import libcchdo.formats.ctd.polarstern as ctd_polarstern

    PARAMETERS = {
        7: "param_depth_water",
        8: "param_press",
        9: "param_sal",
        10: "param_sigma_theta",
        11: "param_temp",
        12: "param_tpot",
        13: "param_cond",
        14: "param_nobs",
        15: "param_atten",
        16: "param_ys_fl",
        17: "param_chl_fluores",
    }

    def unpack_param_meta(meta_param):
        return {"units": meta_param[1],
                "pi": meta_param[2],
                "method": meta_param[3],
                "comment": meta_param[4]}

    def unpack_citation(meta_cite):
        return {"name": meta_cite[1],
                "year": meta_cite[2],
                "description": meta_cite[3]}

    def unpack_reference(meta_ref):
        return unpack_citation(meta_ref)

    def unpack_events(meta_events):
        return {"name": meta_events[1],
                "latitude": meta_events[2],
                "longitude": meta_events[3],
                "elevation": meta_events[4],
                "date_time": meta_events[5],
                "location": meta_events[6],
                "campaign": meta_events[7],
                "basis": meta_events[8],
                "device": meta_events[9]}

    # Make sure this database can read its Unicode entries
    db.text_factory = str

    for input_filename in sys.files:
        LOG.info(input_filename)

        meta = {}
        meta_cast = db.cursor().execute(
                "select * from ctd_casts where filename = ? limit 1",
                (os.path.basename(input_filename), )).fetchone()

        if not meta_cast:
            LOG.error(u"no metadata for {0}".format(input_filename))
            continue

        meta["filename"] = meta_cast[1]

        meta["cites"] = unpack_citation(db.cursor().execute(
                "select * from ctd_citations where id = ? limit 1",
                (meta_cast[2], )).fetchone())

        meta["refs"] = unpack_reference(db.cursor().execute(
                "select * from ctd_references where id = ? limit 1",
                (meta_cast[3], )).fetchone())

        meta["events"] = unpack_events(db.cursor().execute(
                "select * from ctd_events where id = ? limit 1",
                (meta_cast[4], )).fetchone())

        meta["min_depth"] = meta_cast[5]

        meta["max_depth"] = meta_cast[6]

        for i in PARAMETERS:
            if meta_cast[i] != 0:
                meta_param = db.cursor().execute(
                        "select * from ctd_%s where id = ? limit 1" %
                        PARAMETERS[i], (meta_cast[i], )).fetchone()
                meta[PARAMETERS[i]] = unpack_param_meta (meta_param)

        output_filename = os.path.basename(input_filename)
        output_filename = output_filename[:output_filename.find('.')] + \
                          "_ct1.csv"

        datafile = ctd_polarstern.read(meta, input_filename)

        if args.commit_to_file:
            with closing(args.ctdex) as out_file:
                try:
                    ctdex.write(datafile, out_file)
                except TypeError:
                    LOG.error(u'{0} {1!r}'.format(input_filename, 
                            map(lambda col: col.parameter.display_order,
                            datafile.columns.values())))
        else:
            print "%sOutput to %s (not written):%s" % ("", output_filename, "")


def _multi_file(reader, files, output, **kwargs):
    dfc = DataFileCollection()
    for f in files:
        d = DataFile()
        reader.read(d, f, **kwargs)
        dfc.files.append(d)
    if output is not sys.stdout:
        output = open(output, 'w')
    ctdzipex.write(dfc, output)


def _single_file(reader, files, output, **kwargs):
    d = DataFile()
    reader.read(d, files[0], **kwargs)
    if output is not sys.stdout:
        output = open(output, 'w')
    ctdex.write(d, output)


def sbe_to_ctd_exchange(args):
    salt, temp, output = ('first', 'first', sys.stdout)
    if args.salt:
        salt = int(args.salt)
    if args.temp:
        temp = int(args.temp)
    if args.output:
        output = args.output

    
    if len(args.files) > 1:
        if output is not sys.stdout:
            output = output + '_ct1.zip'

        _multi_file(sbe, args.files, output, salt=salt, temp=temp)

    if len(args.files) == 1:
        if output is not sys.stdout:
            output = output +  '_ct1.csv'

        _single_file(sbe, args.files, output, salt=salt, temp=temp)


def sbe_asc_to_ctd_exchange(args):
    output, expo = (sys.stdout, '')
    if (args.expo):
        expo = args.expo
    if (args.output):
        output = args.output
    d = DataFile()
    f = args.files
    if len(args.files) == 1:
        if output is not sys.stdout:
            output = output + "_ct1.csv"

        _single_file(asc, args.files, output, expo=expo)

    if len(args.files) > 1:
        if output is not sys.stdout:
            output = output + '_ct1.zip'

        _multi_file(asc, args.files, output, expo=expo)


def australian_navy_ctd(args):
    """Download and convert Australian Navy CTD data."""
    from pydap.client import open_url
    from libcchdo.thredds import crawl
    from libcchdo.formats.ctd.zip import exchange as ctdzipex
    from libcchdo.formats.zip import write as zwrite

    dfcs = []

    cf_param_to_cchdo_param = {
        'sea_water_pressure': 'CTDPRS',
        'sea_water_temperature': 'CTDTMP',
        'sea_water_practical_salinity': 'CTDSAL',
    }
    ignored_qc_flags = [
        'time_qc_flag', 'position_qc_flag',
    ]
    qc_conventions = {
        'Proposed IODE qc scheme March 2012': {
            1: 2, # good
            2: 5, # not_evaluated_or_unknown
            3: 3, # suspect
            4: 4, # bad
            9: 9, # missing
        },
    }

    dfc = DataFileCollection()
    catalog = "http://www.metoc.gov.au/thredds/catalog/RAN_CTD_DATA/catalog.xml"
    for url in crawl(catalog):
        df = DataFile()

        LOG.info(u'Reading %s', url)
        dset = open_url(url)
        vars = dset.keys()
        for vname in vars:
            var = dset[vname]
            attrs = var.attributes
            if 'standard_name' in attrs:
                std_name = attrs['standard_name']
                if std_name == 'time':
                    df.globals['_DATETIME'] = \
                        datetime(1950, 1, 1) + timedelta(var[:])
                elif std_name == 'latitude':
                    df.globals['LATITUDE'] = var[:]
                elif std_name == 'longitude':
                    df.globals['LONGITUDE'] = var[:]
                elif std_name in cf_param_to_cchdo_param:
                    cparam = cf_param_to_cchdo_param[std_name]
                    if '_FillValue' in attrs:
                        fill_value = attrs['_FillValue']
                        values = []
                        for x in var[:]:
                            if equal_with_epsilon(x, fill_value):
                                values.append(None)
                            else:
                                values.append(x)
                    else:
                        values = var[:]

                    try:
                        df[cparam].values = values
                    except KeyError:
                        df[cparam] = Column(cparam)
                        df[cparam].values = values
                elif 'status_flag' in std_name:
                    flagged_param = std_name.replace('status_flag', '').strip()
                    cparam = cf_param_to_cchdo_param[flagged_param]
                    qc_convention = attrs['quality_control_convention']
                    if qc_convention in qc_conventions:
                        qc_map = qc_conventions[qc_convention]
                        df[cparam].flags_woce = [qc_map[x] for x in var[:]]
                else:
                    LOG.debug('unhandled standard_name %s', std_name)
            elif (
                    'long_name' in attrs and
                    attrs['long_name'] == 'profile identifier'):
                profile_id = var[:]
                cruise_id = profile_id / 10 ** 4
                profile_id = profile_id - cruise_id * 10 ** 4
                df.globals['EXPOCODE'] = str(cruise_id)
                df.globals['STNNBR'] = str(profile_id)
                df.globals['CASTNO'] = str(1)
            elif vname in ignored_qc_flags:
                df.globals['_' + vname] = var[:]
            elif (
                    vname.endswith('whole_profile_flag') or 
                    vname.endswith('sd_test')):
                pass
            else:
                LOG.debug('unhandled variable %s', vname)

        # attach new file to appropriate collection
        if dfc.files:
            if dfc.files[0].globals['EXPOCODE'] != df.globals['EXPOCODE']:
                dfcs.append(dfc)
                dfc = DataFileCollection()
        dfc.append(df)

    with closing(args.output) as out_file:
        next_id = 0
        def get_filename(dfc):
            try:
                return '{0}_ct1.zip'.format(dfc.files[0].globals['EXPOCODE'])
            except IndexError:
                next_id += 1
                return '{0}_ct1.zip'.format(next_id)
        zwrite(dfcs, out_file, ctdzipex, get_filename)


class LineRep(object):
    """WOCE line representation on this map.

    WOCE lines are represented by cruises. Sometimes it takes more than one
    cruise to accurately represent the line.

    Roxanne Lee painstakingly gathered this list.

    """
    def __init__(self, name, points, *cruise_expocodes):
        self.name = name
        self.cruises = cruise_expocodes
        if points is None:
            self.points = []
            for track, expo in tracks_for_cruises(*self.cruises):
                self.points.extend(track)
        else:
            self.points = points

    def __unicode__(self):
        return u'LineRep({0}, {1})'.format(self.name, self.cruises)

    def __repr__(self):
        return unicode(self)
    

def read_rep_lines(file):
    rows = csv_reader(file)
    for row in rows:
        yield row


def plot_woce_representation(args, file_path):
    """Take a CSV and plot specified cruises on a map.

    The CSV is formatted as::

    WOCE lines for map
    GenericLine,ExpoCode,Line,Year,SecondaryExpoCode,SecondaryLine,SecondaryYear

    WOCE lines for map
    GenericLine,ExpoCode,Line,Year,SecondaryExpoCode,SecondaryLine,SecondaryYear

    """
    woce_repr_sheet = (
        "https://docs.google.com/spreadsheet/"
        "ccc?key=0AseZhdC_bXrXdFdwbEVCZUpzYUpocUNUQnZVYk00TGc#gid=0")
    basins = OrderedDict()
    try:
        with open(file_path) as ifile:
            basin = None
            for row in read_rep_lines(ifile):
                if (
                        row[0] == 'WOCE lines for map' or
                        row[0] == '' or 
                        row[0] == 'Line'):
                    continue
                if row[1] == '':
                    basin = row[0]
                    continue

                expos = [x for x in [row[1], row[4]] if x != '']
                rep = LineRep(row[0], None, *expos)
                try:
                    basins[basin].append(rep)
                except KeyError:
                    basins[basin] = [rep]
    except (OSError, IOError), err:
        LOG.error(u'Cannot plot without a WOCE representation spreadsheet.')
        LOG.info(
            u'Download the spreadsheet as a CSV from {0} and put it at '
            '{1}'.format(woce_repr_sheet, file_path))
        return

    dot_size = 170
    if args.draft:
        dot_size = 3
    if args.large_dots:
        dot_size *= 2.35

    args, bm, gmt_style = presets_goship(
        gmt_color(0xFF, 0x88, 0x88),
        dot_size=dot_size, args=args, draft=args.draft)

    for basin in basins:
        for linerep in basins[basin]:
            xxx = []
            yyy = []
            for iii, jjj in linerep.points:
                xxx.append(iii)
                yyy.append(jjj)

            xxx, yyy = bm(xxx, yyy)

            if not xxx and not yyy:
                print 'unable to plot ', linerep
            else:
                dots = bm.scatter(xxx, yyy, **gmt_style)
    bm.savefig(args.output_filename)


def flatten_zip(fileobj, outfile):
    """Given a zip file, flatten any directory structure inside it."""
    oldzip = ZeroCommentZipFile(fileobj, 'r')
    newzip = Zip.create(outfile)
    for info in oldzip.infolist():
        fname = info.filename
        newfname = fname
        if fname.find('/') > -1:
            newfname = os.path.basename(fname)
        newinfo = copy(info)
        newinfo.filename = newfname
        newzip.writestr(newinfo, oldzip.read(info))
    newzip.close()
    oldzip.close()
    return fileobj
