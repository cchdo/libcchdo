import struct
import sys
import os
import os.path
import shutil
from tarfile import TarFile
from copy import copy

import filecmp

from tempfile import mkdtemp

from matplotlib.patches import Rectangle

import numpy as np

from sqlalchemy.sql import or_, distinct

from libcchdo import LOG, config, StringIO
from libcchdo.db import connect
from libcchdo.db.model import legacy
from libcchdo.db.model import std
from libcchdo.db.model.legacy import Document
from libcchdo.model.datafile import DataFile, DataFileCollection, Column
from libcchdo.units import convert as ucvt
from libcchdo.formats import add_pre_write
from libcchdo import fns
import libcchdo.formats.summary.woce as sumwoce
import libcchdo.formats.bottle.exchange as botex
import libcchdo.formats.ctd.exchange as ctdex
import libcchdo.formats.ctd.sbe9 as sbe
import libcchdo.formats.ctd.zip.netcdf_oceansites as ctdzipnc_os
import libcchdo.formats.ctd.zip.exchange as ctdzipex


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


def _check_and_replace_parameters_convert(self):
    for column in self.columns.values():
        parameter = column.parameter
        std_parameter = std.find_by_mnemonic(parameter.name)

        if not std_parameter and not parameter.name.startswith('_'):
            L.LOG.warn("Unknown parameter '%s'" % parameter.name)
            continue

        given_units = parameter.units.mnemonic if parameter.units else None
        expected_units = std_parameter.units.mnemonic \
            if std_parameter.units else None
        from_to = (given_units, expected_units)

        if given_units and expected_units and given_units != expected_units:
            L.LOG.warn(("Mismatched units for '%s'. "
                      "Found '%s' but expected '%s'") % \
                      ((parameter.name,) + from_to))
            try:
                unit_converter = self.unit_converters[from_to]
                convert = None
                while not convert or convert.lower() not in ('y', 'n'):
                    try:
                        convert = raw_input(
                            ("Convert from '%s' to '%s' for '%s'? (y/n)") % \
                            (from_to + (parameter.name,)))
                    except EOFError:
                        pass
                if convert == 'y':
                    L.LOG.info("Converting from '%s' -> '%s' for %s." % \
                             (from_to + (column.parameter.name,)))
                    column = unit_converter(self, column)
                else:
                    # Skip conversion and unit change.
                    continue
            except KeyError:
                L.LOG.info(("No unit converter registered with file for "
                          "'%s' -> '%s'. Skipping conversion.") % from_to)
                continue

        column.parameter = std_parameter


def convert_per_litre_to_per_kg(file):
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
        ucvt.milliliter_per_liter_to_umol_per_kg
    file.unit_converters[('UMOL/L', u'UMOL/KG')] = \
        ucvt.mol_per_liter_to_mol_per_kg
    file.unit_converters[('PMOL/L', u'PMOL/KG')] = \
        ucvt.mol_per_liter_to_mol_per_kg
    file.unit_converters[('NMOL/L', u'NMOL/KG')] = \
        ucvt.mol_per_liter_to_mol_per_kg
    # XXX YIKES but it's there in the fortran
    #file.unit_converters[('MMOL/L', u'UMOL/KG')] = \
    #    ucvt.mol_per_liter_to_mol_per_kg
    _check_and_replace_parameters_convert(file)


PRESSURE_COLUMNS = ('CTDPRS', 'CTDRAW', )


def ctd_bacp_xmiss_merge_ctd_exchange(mergefile, file):
    merge_pressure = None
    pressure = None
    for c in PRESSURE_COLUMNS:
        try:
            merge_pressure = mergefile.columns[c]
            pressure = file.columns[c]
        except KeyError:
            pass
    if merge_pressure is None or pressure is None:
        LOG.warn(
            'Unable to find a matching pressure column in both files. Could '
            'not merge.')
        return 1

    xmiss_column = None
    try:
        xmiss_column = file.columns['XMISS']
    except KeyError:
        pass
    if not xmiss_column:
        xmiss_column = file.columns['XMISS'] = Column('XMISS')
        xmiss_column.values = [None] * len(file)

    merge_xmiss = None
    try:
        merge_xmiss = mergefile.columns['XMISS']
    except KeyError:
        pass
    if not merge_xmiss:
        LOG.warn('Merge file has no XMISS column to merge')
        return 1

    for i, p in enumerate(merge_pressure.values):
        j = pressure.values.index(p)
        xmiss_column.values[j] = merge_xmiss.values[i]


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
            dfc = fns.read_arbitrary(file, 'ctdzipex')
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


def _multi_file(files, salt, temp, output):
    dfc = DataFileCollection()
    for f in files:
        d = DataFile()
        sbe.read(d, f, salt, temp)
        dfc.files.append(d)
    if output is not sys.stdout:
        output = open(output, 'w')
    ctdzipex.write(dfc, output)


def _single_file(files, salt, temp, output):
    d = DataFile()
    sbe.read(d, files[0], salt, temp)
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

        _multi_file(args.files, salt, temp, output)

    if len(args.files) == 1:
        if output is not sys.stdout:
            output = output +  '_ct1.csv'

        _single_file(args.files, salt, temp, output)


def _gmt_label_offsets(basemap):
    yoffset = (basemap.urcrnry - basemap.llcrnry) / 100. * 2.5 * basemap.aspect
    xoffset = (basemap.urcrnrx - basemap.llcrnrx) / 100. * 2
    return (xoffset, yoffset)


def create_map_from_argument_parser(args):
    """Creates a map using the arguments from an ArgumentParser."""
    from libcchdo.plot import etopo

    if etopo.is_proj_cylindrical(args.projection):
        m = etopo.create_map(
            args.minutes, 3, 
            projection=args.projection,
            llcrnrlat=args.bounds_cylindrical[1],
            llcrnrlon=args.bounds_cylindrical[0],
            urcrnrlat=args.bounds_cylindrical[3],
            urcrnrlon=args.bounds_cylindrical[2])
    elif etopo.is_proj_pseudocylindrical(args.projection):
        m = etopo.create_map(
            args.minutes, 3, 
            projection=args.projection,
            lon_0=args.bounds_elliptical)
    elif etopo.is_proj_polar(args.projection):
        m = etopo.create_map(
            args.minutes, 3, 
            projection=args.projection,
            boundinglat=60,
            lon_0=(args.bounds_elliptical + 180) % 360)
    else:
        LOG.error(u'Unhandled projection {0}'.format(args.projection))
        return (None, None, None)

    label_parallels = [1, 1, 0, 0]
    label_meridians = [0, 0, 1, 1]
    label_font_size = 9
    label_font_color = 'k'
    label_ny = 5
    label_nx = 6

    xoffset, yoffset = _gmt_label_offsets(m)

    if etopo.is_proj_cylindrical(args.projection):
        parallels = np.linspace(
            args.bounds_cylindrical[3], args.bounds_cylindrical[1], label_ny)
        meridians = np.linspace(
            args.bounds_cylindrical[2], args.bounds_cylindrical[0], label_nx)
    elif etopo.is_proj_pseudocylindrical(args.projection):
        parallels = range(-90, 90, 20)
        meridians = range(
            args.bounds_elliptical, args.bounds_elliptical + 360, 20)
    else:
        parallels = [60, 70, 80, ]
        meridians = range(0, 360, 20)

    parallels = m.drawparallels(parallels, label_font_color, 0.1,
        fmt=etopo.gmt_label_fmt, xoffset=xoffset, yoffset=yoffset,
        labels=label_parallels, fontsize=label_font_size)
    meridians = m.drawmeridians(meridians, label_font_color, 0.1,
        fmt=etopo.gmt_label_fmt, xoffset=xoffset, yoffset=yoffset,
        labels=label_meridians, fontsize=label_font_size)

    if args.fill_continents:
        m.fillcontinents(color='k')

    return (m, parallels, meridians)


def _edit_graticules(m, parallels, meridians,
                     solid_graticules=False, draw_fancy_borders=True):
    """Edit the graticules of the basemap."""
    from libcchdo.plot import etopo
    fancy_linewidth=1
    fancyborder = {
        'meridians': [],
        'parallels': [],
    }

    if not etopo.is_proj_cylindrical(m.projection):
        LOG.error(u'Fancy borders and editing is not yet implemented for '
            'non-cylindrical projections.')
        return fancyborder

    ax = m._check_ax()
    xoffset, yoffset = _gmt_label_offsets(m)

    xticks = []
    ylims = []
    for m in sorted(meridians.keys()):
        lines, labels = meridians[m]
        for line in lines:
            if solid_graticules:
                line.set_dashes((None, None))

            # Need to shorten the meridian lines a bit
            ydata = line.get_ydata()
            xdata = line.get_xdata()
            start = ydata[0] + yoffset
            end = ydata[-1] - yoffset

            istart = 0
            iend = len(ydata) - 1
            it = np.nditer(ydata, flags=['f_index'])
            while not it.finished:
                if it[0] >= start:
                    istart = it.index
                    break
                it.iternext()

            it = np.nditer(ydata[::-1], flags=['f_index'])
            while not it.finished:
                if it[0] <= end:
                    iend = it.index
                    break
                it.iternext()
            ydata = ydata[istart:iend - istart]
            xdata = xdata[istart:iend - istart]
            line.set_ydata(ydata)
            line.set_xdata(xdata)

            xticks.append([xdata[0], xdata[-1]])
            ylims.append([start, end])

    # Make the offsets square
    yoffset /= 1.5
    xoffset = yoffset

    if draw_fancy_borders:
        # Tack on extra boxes for the meridian pass (the four corners)
        xticks = [[xticks[0][0] - xoffset, xticks[0][1] - xoffset]] + \
            xticks + [[xticks[-1][0] + xoffset, xticks[-1][1] + xoffset]]
        ylims = [copy(ylims[0])] + ylims + [copy(ylims[-1])]
        for ylim in ylims:
            # shift bottom row down (rects are defined by lower-left corner)
            ylim[0] -= 0.5 * yoffset
            # shift top row up
            ylim[1] -= 0.45 * yoffset

        lastxs = None
        lastys = None
        for i, (xs, ys) in enumerate(zip(xticks, ylims)):
            if not (lastxs is None and lastys is None):
                rects = zip(zip(lastxs, lastys), zip(xs, ys))
                color = 'w'
                if i % 2 == 0:
                    color = 'k'
                for rect in rects:
                    w = rect[1][0] - rect[0][0]
                    r = Rectangle(rect[0], w, yoffset,
                        alpha=1, antialiased=False, linewidth=fancy_linewidth,
                        facecolor=color)
                    ax.add_patch(r)
                    fancyborder['meridians'].append(r)
            lastxs = xs
            lastys = ys

    xlims = []
    yticks = []
    for i, p in enumerate(sorted(parallels.keys())):
        lines, labels = parallels[p]
        border = False
        if i == 0 or i == len(parallels.keys()) - 1:
            border = True
        for line in lines:
            if solid_graticules:
                line.set_dashes((None, None))

            xdata = line.get_xdata()
            ydata = line.get_ydata()

            if draw_fancy_borders and border:
                line.remove()

            xlims.append([xdata[0], xdata[-1]])
            # Shift slightly up to get things to line up
            yticks.append([
                ydata[0] + yoffset * 0.05,
                ydata[-1] + yoffset * 0.05])
    for xlim in xlims:
        xlim[0] -= xoffset

    if draw_fancy_borders:
        lastxs = None
        lastys = None
        for i, (xs, ys) in enumerate(zip(xlims, yticks)):
            if not (lastxs is None and lastys is None):
                rects = zip(zip(lastxs, lastys), zip(xs, ys))
                color = 'w'
                if i % 2 == 0:
                    color = 'k'
                for rect in rects:
                    h = rect[1][1] - rect[0][1]
                    r = Rectangle(rect[0], xoffset, h,
                        alpha=1, antialiased=False, linewidth=fancy_linewidth,
                        facecolor=color)
                    ax.add_patch(r)
                    fancyborder['parallels'].append(r)
            lastxs = xs
            lastys = ys
    return fancyborder


def plot_etopo(args):
    from libcchdo.plot import etopo
    plt = etopo.plt

    from matplotlib import rc
    rc('font',
        **{
            'family': 'sans-serif',
            'sans-serif': ['Helvetica'],
        })
    
    if etopo.is_proj_cylindrical(args.projection):
        title_text = '{}; {}'.format(
            args.bounds_cylindrical[0:2], args.bounds_cylindrical[2:4])
    elif etopo.is_proj_pseudocylindrical(args.projection):
        title_text = 'center {}'.format(args.bounds_elliptical)
    else:
        title_text = args.projection
    title_text = 'This is a plot of the world ' + title_text

    m, parallels, meridians = create_map_from_argument_parser(args)

    ax = m._check_ax()
    ax.set_autoscale_on(True)
    if etopo.is_proj_cylindrical(args.projection):
        ax.margins(0.06, 0.03)
    elif etopo.is_proj_pseudocylindrical(args.projection):
        ax.margins(0.05, 0.06)
    elif etopo.is_proj_polar(args.projection):
        ax.margins(0.03, 0.03)
    else:
        LOG.warn(
            u'Cannot set margins for unhandled projection {0}'.format(
                args.projection))

    ax.set_title(title_text, size=18, position=(0.5, 1), fontweight='bold')

    fancyborder = _edit_graticules(m, parallels, meridians)

    LOG.info('rasterizing')

    plt.savefig(args.output_filename,
        dpi=etopo.preset_dpi(str(args.width)),
        # XXX
        #transparent=True,
        format='png', bbox_inches='tight', pad_inches=0.1)

    # XXX
    #import pdb; pdb.set_trace()
