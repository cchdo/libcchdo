#!/usr/bin/env python

import os
import sys
from gzip import GzipFile
from math import floor, fsum, atan2, sin, cos
from urllib2 import urlopen, URLError
from tempfile import SpooledTemporaryFile
from copy import copy

import numpy as np
from numpy import ma, arange

# XXX HACK
# scipy and matplotlib both import PIL causing it to collide
# http://jaredforsyth.com/blog/2010/apr/28/
#     accessinit-hash-collision-3-both-1-and-1/
import PIL.Image
sys.modules['Image'] = PIL.Image

from scipy.ndimage.interpolation import map_coordinates

from netCDF4 import Dataset

import matplotlib
from matplotlib import rcParams, rc
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Rectangle, Circle, Arc, Polygon

from mpl_toolkits.basemap import (
    Basemap, shiftgrid, _pseudocyl, _cylproj, 
    )

from libcchdo import config, LOG
from libcchdo.plot.interpolation import BicubicConvolution


etopo_root = 'http://www.ngdc.noaa.gov/mgg/global'


etopo1_root = etopo_root + '/relief/ETOPO1/data'


etopo_dir = os.path.join(config.get_config_dir(), 'etopos')


def is_proj_cylindrical(proj):
    return proj in _cylproj


def is_proj_pseudocylindrical(proj):
    return proj in _pseudocyl


projections_polar = [
    'spstere', 'npstere', 'splaea', 'nplaea', 'spaeqd', 'npaeqd']


def is_proj_polar(proj):
    return proj in projections_polar


def get_nx_ny(basemap, lons, cut_ratio=1):
    """ Gives the number of points in the basemap per side

        Adapted from part of basemap.wrapimage

    """
    nlons = len(lons)
    if basemap.projection != 'cyl':
        # 4 makes superfast and still good quality
        dx = 2. * np.pi * basemap.rmajor / float(nlons) * cut_ratio
        LOG.debug('not cyl\t%f' % dx)
        nx = int((basemap.xmax - basemap.xmin) / dx) + 1
        ny = int((basemap.ymax - basemap.ymin) / dx) + 1
    else:
        dx = 360. / float(nlons) * cut_ratio
        LOG.debug('cyl\t%f' % dx)
        nx = int((basemap.urcrnrlon - basemap.llcrnrlon) / dx) + 1
        ny = int((basemap.urcrnrlat - basemap.llcrnrlat) / dx) + 1
    return nx, ny


def mask_proj_bounds(self, img, lons, lats, nx, ny, tx, ty):
    """ Gives the image with the area outside the projection area masked

        This function is adapted from parts of basemap.wrapimage to work in 3
        dimensions instead of 6. (x, y, z) vs (x, y, r, g, b, a)

        Arguments:
        self - a Basemap
        img - an image to mask
        lons - the lons of the img
        lats - the lats of the img
        nx - 
        ny -
        tx - the transformed lons that correspond to the basemap
        ty - the transformed lats that correspond to the basemap

    """
    bmproj = self.projection == 'cyl' and \
             self.llcrnrlon == -180 and self.urcrnrlon == 180 and \
             self.llcrnrlat == -90 and self.urcrnrlat == 90
    if bmproj: # interpolation necessary.
        return

    lonsr, latsr = self(tx, ty, inverse=True)
    mask = ma.zeros((ny, nx), np.int8)

    # for ortho,geos mask pixels outside projection limb.
    if self.projection in ['geos', 'ortho', 'nsper'] or \
       (self.projection == 'aeqd' and self._fulldisk):
        mask[:,:,0] = np.logical_or(lonsr > 1.e20, latsr > 1.e30)
    # treat pseudo-cyl projections such as mollweide, robinson and sinusoidal.
    elif is_proj_pseudocylindrical(self.projection):
        lon_0 = self.projparams['lon_0']
        lonright = lon_0 + 180.
        lonleft = lon_0 - 180.
        x1 = np.array(ny * [0.5 * (self.xmax + self.xmin)], np.float)
        y1 = np.linspace(self.ymin, self.ymax, ny)
        lons1, lats1 = self(x1, y1, inverse=True)
        lats1 = np.where(lats1 < -89.999999, -89.999999, lats1)
        lats1 = np.where(lats1 > 89.999999, 89.999999, lats1)
        for j, lat in enumerate(lats1):
            xmax, ymax = self(lonright, lat)
            xmin, ymin = self(lonleft, lat)
            mask[j,:] = np.logical_or(tx[j,:] > xmax, tx[j,:] < xmin)
    elif is_proj_polar(self.projection):
        # Mask the data so only a circle remains
        cx = nx / 2
        cy = ny / 2

        # Add a little extra so low-res data doesn't get choppy at edges
        r = cx ** 2 * 1.03
        def in_circle(x, y):
            return ((x - cx) ** 2 + (y - cy) ** 2) < r
            
        for i in range(nx):
            for j in range(ny):
                if not in_circle(i, j):
                    mask[i, j] = 1

    newimg = ma.masked_array(img, mask=mask)
    return newimg


def downsample(scale, xs, ys, zs):
    """ Downsample ETOPO data to the given scale

        scale - percentage of points to keep
            Percentages corresponding to ETOPO levels

            Percentage | ETOPO level
            -----------+------------
            1          | ETOPO1
            0.5        | ETOPO2
            0.1        | ETOPO5

    """
    LOG.info('Downsampling to %f' % scale)
    newxs = np.linspace(xs[0], xs[-1], xs.size * scale)
    newys = np.linspace(ys[0], ys[-1], ys.size * scale)

    # This is incredibly slow due to massive memory use
    #from scipy.interpolate import RectBivariateSpline
    #fzs = RectBivariateSpline(ys, xs, zs)
    #newzs = fzs(newxs, newys).T

    # This shows slightly better coastline resolution around areas such as N
    # Australia and N Siberia
    #fzs = BicubicConvolution(xs, ys, zs)
    #newzs = np.array(fzs(newxs, newys))

    # Extremely fast way to downsample
    newzs = map_coordinates(
        zs.T,
        np.meshgrid(np.linspace(0, xs.size + 1, xs.size * scale),
                    np.linspace(0, ys.size + 1, ys.size * scale)),
        mode='wrap')

    return newxs, newys, newzs


def etopo_filename(arcmins=1, version='ice', format='gmt', gz=False):
    assert type(arcmins) is int
    assert 1 <= arcmins and arcmins <= 10800
    assert version in ('ice', 'bed', )

    args = {
        'arcmins': arcmins,
        'version': version.capitalize(),
    }
    if format == 'gmt':
        args['format'] = 'gmt4'
    elif format == 'gdal':
        args['format'] = 'gdal'
    if gz:
        args['gz'] = '.gz'
    else:
        args['gz'] = ''
    return 'ETOPO{arcmins:d}_{version}_g_{format}.grd{gz}'.format(**args)


def etopo1_url(version='ice', registration='grid', binformat='netcdf',
              format='gmt'):
    assert registration in ('grid', 'cell', )
    assert binformat in (
        'netcdf', 'binary', 'geodas_g98', 'xyz', 'georeferenced_tiff', )

    filename = etopo_filename(version=version, format=format, gz=True)
    if version == 'ice':
        version_dir = 'ice_surface'
    elif version == 'bed':
        version_dir = 'bedrock'
    
    return '{root}/{ver}/{reg}_registered/{bin}/{fn}'.format(
        root=etopo1_root, ver=version_dir, reg=registration, bin=binformat,
        fn=filename)


def download_etopo1(etopo_path, version='ice'):
    url = etopo1_url(version)
    LOG.info(
        '%s was not found. Attempting to download from %s.' % (etopo_path, url))
    try:
        response = urlopen(url)

        content_length = -1
        blocksize = 2 ** 22
        blocksize_disk = 2 ** 25

        info = response.info()
        headers = info.headers
        content_lengths = filter(lambda x: x.startswith('Content-Length'),
                                 headers)
        if content_lengths:
            content_length = content_lengths[0]
            content_length = int(content_length.strip().split()[1])

        LOG.info('Downloading %d bytes at %d byte chunks' % (content_length,
                                                             blocksize))
        LOG.info('This will take a while. Go enjoy some fresh air.')

        with SpooledTemporaryFile(2 ** 30) as s:
            bytes = 0
            last_percent = 0
            data = response.read(blocksize)
            while data:
                s.write(data)
                bytes += len(data)
                data = response.read(blocksize)

                percent = float(bytes) / content_length
                if percent > last_percent + 0.05:
                    LOG.debug('%d / %d = %f' % (bytes, content_length, percent))
                    last_percent = percent
            response.close()
            s.flush()
            s.seek(0)

            LOG.debug('Gunzipping file to %s' % etopo_path)
            g = GzipFile(fileobj=s)
            bytes = 0
            with open(etopo_path, 'wb') as f:
                data = g.read(blocksize_disk)
                while data:
                    f.write(data)
                    data = g.read(blocksize_disk)
                    bytes += len(data)
                    LOG.debug('%d written' % bytes)

    except URLError, e:
        LOG.critical('Download from %s failed.' % url)
        raise e


def write_etopo_cache(arcmins, version, lons, lats, topo):
    LOG.debug('writing etopo cache %d %s %s %s' % (arcmins, lons, lats, topo))

    etopo_path = os.path.join(etopo_dir, etopo_filename(arcmins, version))
    toponc = Dataset(etopo_path, 'w')

    dim_x = toponc.createDimension('x', lons.size)
    dim_y = toponc.createDimension('y', lats.size)

    var_lons = toponc.createVariable('x', 'f8', ('x', ))
    var_lons.long_name = 'Longitude'
    var_lons.actual_range = [-180., 180.]
    var_lons.units = 'degrees'
    var_lats = toponc.createVariable('y', 'f8', ('y', ))
    var_lats.long_name = 'Latitude'
    var_lats.actual_range = [-90., 90.]
    var_lats.units = 'degrees'
    var_topo = toponc.createVariable('z', 'i4', ('y', 'x', ),
                                     fill_value=-2 ** 31)
    var_topo.long_name = 'z'
    var_topo.actual_range = [topo.min(), topo.max()]

    toponc.Conventions = 'COARDS/CF-1.0'
    toponc.title = etopo_path
    toponc.history = 'downsampled from ETOPO1 by libcchdo'
    toponc.GMT_version = '4.4.0'
    toponc.node_offset = 0

    try:
        LOG.info('Writing %s' % etopo_path)
        var_lons[:] = lons
        var_lats[:] = lats
        var_topo[:] = topo
    finally:
        toponc.close()


def read_etopo(etopo_path):
    toponc = Dataset(etopo_path, 'r')
    try:
        LOG.info('Reading %s' % etopo_path)
        lons = toponc.variables['x'][:]
        lats = toponc.variables['y'][:]
        # Topo is indexed by y then x
        topo = toponc.variables['z'][:]
    finally:
        toponc.close()
    return lons, lats, topo


def etopo(arcmins=1, version='ice', force_resample=False, cache_allowed=True):
    """ Read and normalize etopo data from a netCDF file.

        ETOPO1 can be downloaded from here:
        http://www.ngdc.noaa.gov/mgg/global/global.html

        Please pick from the grid registered files for GMT.

        Args:
            arcmins - the number of arc-minutes between each tick of the grid
            version - whether to use ice surface (ice) or bedrock (bed) relief

        Returns:
            a tuple with (elevation data, lons, lats,
                          offset_from_ground_to_consider_underwater)

    """
    etopo_path = os.path.join(etopo_dir, etopo_filename(arcmins, version))

    LOG.debug('reading {0}'.format(etopo_path))
    LOG.debug('exists? {0}'.format(os.path.isfile(etopo_path)))
    if force_resample or not os.path.isfile(etopo_path):
        if arcmins is 1:
            download_etopo1(etopo_path, version)
            data = read_etopo(etopo_path)
        else:
            etopo_path = os.path.join(
                etopo_dir, etopo_filename(version=version))
            if not os.path.isfile(etopo_path):
                download_etopo1(etopo_path, version)
            data = read_etopo(etopo_path)
            data = downsample(1. / arcmins, *data)
            if cache_allowed:
                write_etopo_cache(arcmins, version, *data)
    else:
        data = read_etopo(etopo_path)
    lons, lats, topo = data
    LOG.info('lons: %s' % (lons.shape))
    LOG.info('lats: %s' % (lats.shape))
    LOG.info('topo: %s %s' % (topo.shape, topo))

    # offset bumps the level just above sea level to below so as to err on
    # the side of less land shown
    offset = 0.
    return (topo, lons, lats, offset)


def etopo_ground_point(topo, etopo_offset=0):
    mint = float(np.min(topo))
    maxt = float(np.max(topo))

    shift = etopo_offset - mint

    # range of topo goes from ~ -10k to ~8k
    # let -10k be 0 %
    # let 8k = 100%
    # 0 = x%
    # 10k / (8k + 10k) = x%
    groundpt = shift / (maxt + shift)
    LOG.debug('topo range [%d, %d]' % (mint, maxt))
    LOG.debug(
        'offset: %f = shift: %f = ground: %f' % (etopo_offset, shift, groundpt))
    return groundpt


def colormap_grayscale(topo, etopo_offset=0):
    """Return a colormap based on the topo range and offset that grayscale.

    """
    LOG.info(u'Generating grayscale colormap for ETOPO')
    locolor = (0.55, 0.55, 0.55)
    hicolor = (0.945, 0.945, 0.945)
    ground_color = (0, 0, 0)
    mount_color = (0.1, 0.1, 0.1)

    groundpt = etopo_ground_point(topo, etopo_offset=10)

    colormap = LinearSegmentedColormap.from_list(
        'cchdo_grayscale',
        ((0, locolor),
         (groundpt, hicolor),
         (groundpt, ground_color),
         (1, mount_color)
        )
    )
    colormap.set_bad(color='k', alpha=0.0)
    colormap.set_over(color='y')
    colormap.set_under(color='c')
    return colormap


def colormap_cberys(topo, etopo_offset=0):
    """ Gives a colormap based on the topo range and offset that is based off
        an original color scheme created by Carolina Berys.

    """
    LOG.info(u'Generating cberys colormap for ETOPO')
    locolor = (0.173, 0.263, 0.898)
    locolor = (0.130, 0.220, 0.855)
    hicolor = (0.549019607843137, 0.627450980392157, 1)
    hicolor = (0.509019607843137, 0.587450980392157, 1)
    ground_color = (0, 0, 0)
    mount_color = (0.1, 0.1, 0.1)

    groundpt = etopo_ground_point(topo, etopo_offset=10)

    colormap = LinearSegmentedColormap.from_list(
        'cchdo_cberys',
        ((0, locolor),
         (groundpt, hicolor),
         (groundpt, ground_color),
         (1, mount_color)
        )
    )
    colormap.set_bad(color='k', alpha=0.0)
    colormap.set_over(color='y')
    colormap.set_under(color='c')
    return colormap


def colormap_ushydro(topo, etopo_offset=0):
    """Return a colormap based on the USHYDRO cruise map

    """
    LOG.info(u'Generating USHYDRO colormap for ETOPO')
    locolor = (0.51, 0.64, 0.72)
    hicolor = (0.85, 0.95, 0.96)
    ground_color = (0, 0, 0)
    mount_color = (0.1, 0.1, 0.1)

    groundpt = etopo_ground_point(topo, etopo_offset=0)

    colormap = LinearSegmentedColormap.from_list(
        'cchdo_ushydro',
        ((0, locolor),
         (groundpt, hicolor),
         (groundpt, ground_color),
         (1, mount_color)
        )
    )
    colormap.set_bad(color='k', alpha=0.0)
    colormap.set_over(color='y')
    colormap.set_under(color='c')
    return colormap


def colormap_goship(topo, etopo_offset=0):
    """Return a colormap based on the GO-SHIP map.

    """
    LOG.info(u'Generating GOSHIP colormap for ETOPO')
    locolor = (0.803, 0.952, 0.976)
    hicolor = (0.921, 0.98, 0.988)
    ground_color = (0.384, 0.384, 0.384)
    mount_color = (0.404, 0.404, 0.404)

    groundpt = etopo_ground_point(topo, etopo_offset=0)

    colormap = LinearSegmentedColormap.from_list(
        'goship',
        ((0, locolor),
         (groundpt, hicolor),
         (groundpt, ground_color),
         (1, mount_color)
        )
    )
    colormap.set_bad(color='k', alpha=0.0)
    colormap.set_over(color='y')
    colormap.set_under(color='c')
    return colormap


colormaps = {
    'gray': colormap_grayscale,
    'cberys': colormap_cberys,
    'ushydro': colormap_ushydro,
    'goship': colormap_goship,
}


def gmt_label_fmt(lon):
    """Format latlons with negative and no positive.

    E.g.
    -10deg -5deg 0deg 5deg 10deg

    """
    while lon > 180:
        lon -= 360
    while lon < -180:
        lon += 360

    lonlabstr = ''
    if lon > 180:
        if rcParams['text.usetex']:
            lonlabstr = r'${\/-%g\/^{\circ}}$'
        else:
            lonlabstr = u'-%g\N{DEGREE SIGN}'
    else:
        if rcParams['text.usetex']:
            lonlabstr = r'${\/%g\/^{\circ}}$'
        else:
            lonlabstr = u'%g\N{DEGREE SIGN}'
    return lonlabstr % lon
    

def preset_dpi(level='240'):
    """ Gives a dpi estimate for matplotlib.pyplot.saveimg given some height
    bound.
    """
    # this function seems to be linear
    if level == '1024':
        return 343.5
    if level == '720':
        return 229
    if level == '480':
        return 152.7
    if level == '320':
        return 102
    if level == '240':
        return 76.5
    return int(level)


class ETOPOBasemap(Basemap):
    GMT_STYLE_DOTS = dict(
        s=18, c='r', antialiased=True, linewidth=0.4, zorder=400)

    GMT_STYLE_LINE = dict(c='k', linewidth=1, antialiased=True, zorder=300)

    def __init__(self, **kwargs):
        super(ETOPOBasemap, self).__init__(**kwargs)
        self.hide_axes_borders()
        self._graticules_labeled = False

    @classmethod
    def new_from_projection(cls, projection, args, **kwargs):
        if is_proj_cylindrical(projection):
            newcls = cls(
                projection=projection,
                llcrnrlat=args.bounds_cylindrical[1],
                llcrnrlon=args.bounds_cylindrical[0],
                urcrnrlat=args.bounds_cylindrical[3],
                urcrnrlon=args.bounds_cylindrical[2], **kwargs)
        elif is_proj_pseudocylindrical(projection):
            newcls = cls(
                projection=projection,
                lon_0=args.bounds_elliptical, **kwargs)
        elif is_proj_polar(projection):
            boundinglat = 60
            lon_0 = (args.bounds_elliptical + 180) % 360
            if projection[0] == 's':
                boundinglat = -30
                lon_0 = args.bounds_elliptical
            newcls = cls(
                projection=projection,
                boundinglat=boundinglat,
                lon_0=lon_0, **kwargs)
            newcls.boundinglat = boundinglat
        elif projection == 'tmerc':
            newcls = cls(
                projection=projection,
                llcrnrlat=args.bounds_cylindrical[1],
                llcrnrlon=args.bounds_cylindrical[0],
                urcrnrlat=args.bounds_cylindrical[3],
                urcrnrlon=args.bounds_cylindrical[2], 
                lon_0=-4.36,
                lat_0=54.7,
                **kwargs)
        else:
            LOG.error(u'Unhandled projection {0}'.format(args.projection))
            newcls = None
        return newcls

    @classmethod
    def new_from_argparser(cls, args, **kwargs):
        """Creates a map using the arguments from an ArgumentParser."""
        newcls = cls.new_from_projection(args.projection, args, **kwargs)
        if newcls:
            newcls.draw_from_argparser(args)
        return newcls

    @property
    def fig(self):
        return plt.gcf()

    @property
    def axes(self):
        """Return the Basemap's matplotlib.axes.Axes."""
        return self._check_ax()

    def add_title(self, text, size=28):
        """Add a title to the plot."""
        # TODO get the title to be bold. fontweight doesn't work...
        self.axes.set_title(
            text, size=size, position=(0.5, 1), fontweight='bold')

    def hide_axes_borders(self):
        """Hide and remove borders that have been added to the plot."""
        # This call should cover projections that have rectangular borders
        self.axes.set_axis_off()

        # Pseudo-cylindrical borders are polygons
        if self.is_proj_pseudocylindrical:
            children = filter(
                lambda c: c.__class__ == Polygon, self.axes.get_children())
            for child in children:
                child.set_linewidth(0)
        # TODO There are elliptical borders as well

    @property
    def is_proj_cylindrical(self):
        return is_proj_cylindrical(self.projection)

    @property
    def is_proj_pseudocylindrical(self):
        return is_proj_pseudocylindrical(self.projection)

    @property
    def is_proj_polar(self):
        return is_proj_polar(self.projection)

    @property
    def is_proj_southern(self):
        return self.is_proj_polar and self.projection[0] == 's'

    @property
    def gmt_label_offsets(self):
        """Gives offsets to space fancy border labels away from the border."""
        if self.is_proj_cylindrical:
            yoffset = (self.urcrnry - self.llcrnry) / 100. * 2.6 * self.aspect
            xoffset = (self.urcrnrx - self.llcrnrx) / 100. * 1.5
        elif self.is_proj_pseudocylindrical:
            yoffset = (self.urcrnry - self.llcrnry) / 100. * 2.5 * self.aspect
            xoffset = (self.urcrnrx - self.llcrnrx) / 100. * 1
        elif self.is_proj_polar:
            yoffset = (self.urcrnry - self.llcrnry) / 100. * 2.5 * self.aspect
            xoffset = (self.urcrnrx - self.llcrnrx) / 100. * 2
        else:
            yoffset = 0
            xoffset = 0
        return (xoffset, yoffset)

    def draw_etopo(self, etopo_scale, cut, cmtopo=colormap_cberys,
                   version='ice', force_resample=False):
        """Draw an etopo bathymetry overlay on the Basemap.

        Arguments:
            etopo_scale - the scale of the etopo data
            cut - the ratio of points to cut out. The higher this is the more
                averaging of the etopo will occur. Values over 10 will generally
                result in a poor map and values under 3 will result in a very
                long transform time.

        """
        topo, lons, lats, etopo_offset = etopo(
            etopo_scale, version, force_resample)

        if cut is None:
            if etopo_scale == 1:
                cut = 6
            else:
                cut = 4

        LOG.debug('Cut ratio %d' % cut)
        nx, ny = get_nx_ny(self, lons, cut_ratio=cut)
        LOG.debug('nx: %d, ny: %d' % (nx, ny))

        if lons.min() < self.llcrnrlon and self.is_proj_cylindrical:
            LOG.info('Shifting grid')
            topo, lons = shiftgrid(self.llcrnrlon, topo, lons)

        LOG.info('Transforming grid')
        #LOG.debug('lons: %s lats: %s nx: %d ny: %d' % (lons, lats, nx, ny))
        topo, tx, ty = self.transform_scalar(
            topo, lons, lats, nx, ny, returnxy=True)

        LOG.info('Masking projection bounds')
        topo = mask_proj_bounds(self, topo, lons, lats, nx, ny, tx, ty)

        self.imshow(topo, cmap=cmtopo(topo, etopo_offset))

    def draw_from_argparser(self, args):
        """Draw based on argparser arguments."""
        try:
            cmtopofn = colormaps[args.cmap]
        except KeyError:
            LOG.warn(
                u'Unknown colormap {0!r}. Defaulted to cberys'.format(
                args.cmap))
            cmtopofn = colormap_cberys

        etopo_cut = 3
        etopo_version = 'ice'
        fillcontinents_kwargs = {'color': 'k'}
        if not args.no_etopo:
            self.draw_etopo(
                args.minutes, etopo_cut, version=etopo_version, cmtopo=cmtopofn)

        if args.fill_continents:
            self.fillcontinents(fillcontinents_kwargs)

    def set_axes_limits(self, ax=None):
        """Extend Basemap set axes limits to account for graticule labels.

        Expand the margin if the graticules are labeled.

        """
        super(ETOPOBasemap, self).set_axes_limits(ax)
        if not self._graticules_labeled:
            return

        # the axis margins need to be shifted so they show up.
        if self.is_proj_cylindrical:
            margins = (0.07, 0.02)
        elif self.is_proj_pseudocylindrical:
            margins = (0.05, 0.06)
        elif self.is_proj_polar:
            margins = (0.03, 0.03)
        else:
            LOG.warn(
                u'Cannot set margins for unhandled projection {0}'.format(
                    self.projection))
            return 

        LOG.debug(
            u'Set axis margins for projection {0} to {1}'.format(
                self.projection, margins))
        # Let the plot axes settle around the plot first before adding
        # margins
        self.axes.autoscale()
        self.axes.margins(*margins)
        
    def get_graticule_ticks(self, label_nx=6, label_ny=5,
                            meridian_spacing=None,
                            parallel_spacing=None):
        """Return tick marks for graticules within the current bounds."""
        LOG.info(u'Determining graticule ticks')
        if self.is_proj_cylindrical:
            parallels = np.linspace(self.urcrnrlat, self.llcrnrlat, label_ny)
            meridians = np.linspace(self.urcrnrlon, self.llcrnrlon, label_nx)
        elif self.is_proj_pseudocylindrical:
            if not meridian_spacing:
                meridian_spacing = 20
            if not parallel_spacing:
                parallel_spacing = 20
            parallels = range(-90, 90, parallel_spacing)
            mstart = int(self.llcrnrlon / 2)
            mstart /= meridian_spacing * meridian_spacing
            meridians = range(mstart, mstart + 360, meridian_spacing)
        else:
            if not meridian_spacing:
                meridian_spacing = 20
            if not parallel_spacing:
                parallel_spacing = 10
            if self.is_proj_southern:
                parallels = range(self.boundinglat, -80, -parallel_spacing)
            else:
                parallels = range(self.boundinglat, 90, parallel_spacing)
            meridians = range(0, 360, meridian_spacing)
        return meridians, parallels

    def draw_graticules(self, meridian_ticks, parallel_ticks,
                        label_font_size=15,
                        label_font_color = 'k',
                        label_meridians=[0, 0, 1, 1],
                        label_parallels=[1, 1, 0, 0],
                        line_width=0.10,
                        line_solid=True,
                        latmax=85):
        """Draw graticules on a Basemap according to an ArgumentParser.

        The label settings are set up so that gmt_graticules can do its job with
        less editing.

        The default settings approximate GMT's fancy border settings.

        Doesn't apply exactly to polar projections b/c the parallels don't get
        labelled.

        latmax - This is the effective inner-most ring for north pole
            projections and the default matches GMT's setting.

        """
        line_dashes = [1, 1]
        if line_solid:
            line_dashes = [1, 0]

        xoffset, yoffset = self.gmt_label_offsets
        artists = {}
        artists['parallels'] = self.drawparallels(
            parallel_ticks, label_font_color, line_width, dashes=line_dashes, 
            latmax=latmax,
            fmt=gmt_label_fmt, xoffset=xoffset, yoffset=yoffset,
            labels=label_parallels, fontsize=label_font_size)
        artists['meridians'] = self.drawmeridians(
            meridian_ticks, label_font_color, line_width, dashes=line_dashes,
            latmax=latmax, 
            fmt=gmt_label_fmt, xoffset=xoffset, yoffset=yoffset,
            labels=label_meridians, fontsize=label_font_size)

        self._graticules_labeled = any(label_parallels + label_meridians)

        self.set_axes_limits()
        return artists

    def gmt_graticules(self, graticules, draw_fancy_borders=True,
                       border_ratio=0.012, border_linewidth=9):
        """Edit the graticules of the basemap to act like GMT graticules.

        Also handles drawing GMT fancy borders.

        """
        parallels = graticules['parallels']
        meridians = graticules['meridians']

        fancy_linewidth=1
        fancyborder = {
            'meridians': [],
            'parallels': [],
        }

        ax = self.axes

        if self.is_proj_cylindrical:
            xoffset, yoffset = self.gmt_label_offsets

            # Make the offsets square 2/3 of distance to label xoffset
            xoffset /= 1.5
            yoffset = xoffset

            # Actual ys leave a line at top obscured and a line at bottom empty.
            # Shift the borders up a little.
            yshift = xoffset / 10

            xlims = []
            yticks = []
            for i, p in enumerate(sorted(parallels.keys())):
                lines, labels = parallels[p]
                is_border = False
                if i == 0 or i == len(parallels.keys()) - 1:
                    is_border = True

                for line in lines:
                    xdata = line.get_xdata()
                    ydata = line.get_ydata()

                    xlims.append([xdata[0] - xoffset, xdata[-1]])
                    yticks.append([ydata[0] + yshift, ydata[-1] + yshift])

                    if draw_fancy_borders and is_border:
                        line.remove()

            if draw_fancy_borders:
                # Tack on extra boxes for the parallel pass (the four corners)
                yticks = [[yticks[0][0] - xoffset, yticks[0][1] - yoffset]] + \
                    yticks + \
                    [[yticks[-1][0] + xoffset, yticks[-1][1] + yoffset]]
                xlims = [copy(xlims[0])] + xlims + [copy(xlims[-1])]

                lastxs = None
                lastys = None
                for i, (xs, ys) in enumerate(zip(xlims, yticks)):
                    if not (lastxs is None and lastys is None):
                        rects = zip(zip(lastxs, lastys), zip(xs, ys))
                        color = 'w'
                        if i % 2 == 1 and not (i == 0 or i == len(rects) - 1):
                            color = 'k'
                        for rect in rects:
                            h = rect[1][1] - rect[0][1]
                            r = Rectangle(
                                rect[0], xoffset, h, alpha=1, antialiased=False,
                                linewidth=fancy_linewidth, facecolor=color)
                            ax.add_patch(r)
                            fancyborder['parallels'].append(r)
                    lastxs = xs
                    lastys = ys

            # The limits for y are those of the yticks
            start = yticks[1][1]
            end = yticks[-2][1]

            xticks = []
            ylims = []
            for i, m in enumerate(sorted(meridians.keys())):
                lines, labels = meridians[m]
                is_border = False
                if i == 0 or i == len(meridians.keys()) - 1:
                    is_border = True

                for line in lines:
                    # Shorten the meridian lines a bit
                    ydata = line.get_ydata()
                    xdata = line.get_xdata()

                    mask = np.logical_and(start < ydata, ydata < end)
                    ydata = ydata[mask]
                    xdata = xdata[mask]

                    line.set_ydata(ydata)
                    line.set_xdata(xdata)

                    xticks.append([xdata[0], xdata[-1]])
                    ylims.append([start - yoffset, end])

                    if draw_fancy_borders and is_border:
                        line.remove()

            if draw_fancy_borders:
                lastxs = None
                lastys = None
                for i, (xs, ys) in enumerate(zip(xticks, ylims)):
                    if not (lastxs is None and lastys is None):
                        rects = zip(zip(lastxs, lastys), zip(xs, ys))
                        # TODO This isn't exactly how GMT does the alternation
                        # (seems to start black on 0-10deg) but it will do for
                        # now.
                        color = 'w'
                        if i % 2 == 0:
                            color = 'k'
                        for rect in rects:
                            w = rect[1][0] - rect[0][0]
                            r = Rectangle(rect[0], w, yoffset,
                                alpha=1, antialiased=False,
                                linewidth=fancy_linewidth, facecolor=color)
                            ax.add_patch(r)
                            fancyborder['meridians'].append(r)
                    lastxs = xs
                    lastys = ys
        elif self.is_proj_pseudocylindrical:
            # Remove every other meridian label so it isn't so cramped
            # Alternate top and bottom so every meridian is still labeled
            for i, m in enumerate(sorted(meridians)):
                lines, labels = meridians[m]

                if labels:
                    label = labels[(i % 2)]
                    labels.remove(label)
                    label.remove()
        elif self.is_proj_polar:
            # TODO polar doesn't look that great w/o fancy borders. maybe
            # there's a way to mask out imshow without harming the data?
            # perhaps
            # self.round and self._clipcircle(ax, objs)
            minx, maxx = None, None
            miny, maxy = None, None
            labeled = None
            for p in sorted(parallels):
                lines, labels = parallels[p]
                if labels:
                    labeled = (lines, labels)
                for line in lines:
                    xs = line.get_xdata()
                    ys = line.get_ydata()
                    if minx is None:
                        minx = xs.min()
                        maxx = xs.max()
                        miny = ys.min()
                        maxy = ys.max()
                        continue
                    minx = min(xs.min(), minx)
                    maxx = max(xs.max(), maxx)
                    miny = min(ys.min(), miny)
                    maxy = max(ys.max(), maxy)

            # Remove the outer parallel label
            if labeled:
                lines, labels = labeled
                for label in labels:
                    label.remove()

                # The fancy border replaces the outer line
                if draw_fancy_borders:
                    for line in lines:
                        line.remove()

            w = maxx - minx
            h = maxy - miny

            cx = minx + w / 2.
            cy = miny + h / 2.
            # Radii for the border, inner, outer, average
            ri = w / 2.
            ro = ri + w * border_ratio
            ra = (ro - ri) / 2.
            rmid = ri + ra
            rlabel = ro + 3. * ra

            linewidth = 1
            border_color = 'k'

            if draw_fancy_borders:
                fancyborder['circles'] = []

                inner = Circle((cx, cy), ri,
                    facecolor='none', linewidth=linewidth,
                    edgecolor=border_color, zorder=110)
                ax.add_patch(inner)
                blank = Circle((cx, cy), rmid,
                    facecolor='none', linewidth=border_linewidth, edgecolor='w',
                    zorder=100)
                ax.add_patch(blank)
                outer = Circle((cx, cy), ro,
                    facecolor='none', linewidth=linewidth,
                    edgecolor=border_color, zorder=110)
                ax.add_patch(outer)

                # Shift the thetas slightly so the border lines up
                if self.is_proj_southern:
                    theta1 = -29.5
                    theta2 = -10.5
                else:
                    theta1 = -9.5
                    theta2 = 9.5

                arcs = []
                for i in range(0, 360, 40):
                    arc = Arc((cx, cy), rmid * 2, rmid * 2, i, theta1, theta2,
                        edgecolor=border_color, linewidth=border_linewidth,
                        zorder=105)
                    arcs.append(arc)
                    ax.add_patch(arc)

                fancyborder['circles'].append(inner)
                fancyborder['circles'].append(outer)
                fancyborder['circles'].append(blank)
                fancyborder['circles'].append(arcs)

            label_font_size = None
            for m in sorted(meridians):
                lines, labels = meridians[m]

                # Also get the label font size
                for label in labels:
                    if label_font_size is None:
                        label_font_size = label.get_fontsize()
                    label.remove()

                label_text = gmt_label_fmt(m)

                # Flip the bottom-half of the labels to face out for legibility
                if self.is_proj_southern:
                    rotation = -m
                    if 90 < m and m < 270:
                        rotation = 180 - m
                else:
                    rotation = 180 + m
                    if (-90 < m and m < 90) or (270 < m and m < 360):
                        rotation = m

                for line in lines:
                    coords = line.get_xydata()

                    # Trim the meridian lines
                    # Meridian lines are drawn from the north pole out. This
                    # means the direction of the lines is flipped for the north
                    # and south poles.
                    r2 = ri ** 2
                    if self.is_proj_southern:
                        for i, (x, y) in enumerate(coords):
                            if (x - cx) ** 2 + (y - cy) ** 2 >= r2:
                                break
                        line.set_xdata(coords[:i, 0])
                        line.set_ydata(coords[:i, 1])
                    else:
                        for i, (x, y) in enumerate(coords):
                            if (x - cx) ** 2 + (y - cy) ** 2 < r2:
                                break
                        line.set_xdata(coords[i:, 0])
                        line.set_ydata(coords[i:, 1])

                    # We know the line given coords[0] and coords[i]. We want to
                    # extend to the distance rlabel
                    pa = [cx, cy]
                    pb = coords[i]
                    theta = atan2(pb[1] - pa[1], pb[0] - pa[0])
                    tx = pa[0] + rlabel * cos(theta)
                    ty = pa[1] + rlabel * sin(theta)

                    ax.text(tx, ty, label_text,
                        horizontalalignment='center',
                        verticalalignment='center', rotation=rotation,
                        fontsize=label_font_size, zorder=200)
        else:
            LOG.error(u'Fancy borders and graticule editing is not yet '
                'implemented for the {0} projection.'.format(self.projection))
        return fancyborder

    def set_gmt_font(self):
        rc('font',
            **{
                'family': 'sans-serif',
                'sans-serif': ['Helvetica'],
            })

    def draw_gmt_fancy_border(self, label_font_size=15,
                              draw_graticules_kwargs={},
                              graticule_ticks_kwargs={},
                              gmt_graticules_kwargs={}):
        """Draw a GMT fancy border around the map plot if able."""
        self.set_gmt_font()
        graticule_ticks = self.get_graticule_ticks(**graticule_ticks_kwargs)
        graticules = self.draw_graticules(
            graticule_ticks[0], graticule_ticks[1],
            label_font_size=label_font_size, **draw_graticules_kwargs)
        fancy_border = self.gmt_graticules(graticules, **gmt_graticules_kwargs)
        return graticule_ticks, graticules, fancy_border

    def resize_figure_to_pixel_width(self, width, ax_height=1.0, ax_xoff=0.0,
                                     ax_yoff=0.01):
        """Resize the current figure to approximate the desired pixel width.

        Also set axes to approximately fill the entire figure.

        """
        figsize = self.fig.get_size_inches()
        ratio = (width / self.fig.dpi) / figsize[0]
        figsize = [ratio * x for x in figsize]
        figsize[1] = figsize[0]
        self.fig.set_size_inches(*figsize)

        self.axes.set_position(
            [ax_xoff, ax_yoff, 1.0 - ax_xoff * 2, ax_height - ax_yoff * 2])
        return ratio

    def savefig(self, filename):
        """Save the figure to a file."""
        LOG.info('Rasterizing...')
        try:
            extent = 'tight'
            plt.savefig(
                filename,
                dpi=self.fig.dpi,
                transparent=True,
                format='png',
                bbox_inches=extent)
        except AssertionError:
            LOG.info(
                u'Matplotlib has a problem with plotting Basemaps that have '
                'nothing on them.')


def plot(args, label_font_size=15, title_font_size=15, basemap_kwargs={},
         draw_graticules_kwargs={}, graticule_ticks_kwargs={},
         gmt_graticules_kwargs={}):
    """Plot using argparse.

    """
    bm = ETOPOBasemap.new_from_argparser(args, **basemap_kwargs)

    if args.title:
        axheight = 0.945
    else:
        axheight = 1.0

    ratio = bm.resize_figure_to_pixel_width(args.width, axheight)
    if ratio < 1:
        label_font_size *= ratio
        title_font_size *= ratio

    if args.title:
        bm.add_title(args.title, title_font_size)
    if not args.no_etopo:
        bm.draw_gmt_fancy_border(
            label_font_size, draw_graticules_kwargs, graticule_ticks_kwargs,
            gmt_graticules_kwargs)
    return bm


def plot_subtitle(line, pi_inst, ship, year):
    """Generate plot subtitle as produced by CCHDO.

    https://bitbucket.org/ghdc/cchdo/wiki/data_curation_plot#!title

    """
    info = {
        'line': line,
        'pi_inst': pi_inst,
        'ship': ship,
        'year': year,
    }
    return '{line} {pi_inst} ({ship} {year})'.format(**info)
    


def plot_line_dots(lons, lats, bm):
    """Plot the datafile coordinates on basemap.

    Returns:
        (line_connecting_dots, [dots])
    
    """
    if not (lats and lons):
        LOG.error(u'Cannot plot file without coordinate data')
        return
    lats = map(float, lats)
    lons = map(float, lons)
    xs, ys = bm(lons, lats)

    line = bm.plot(xs, ys, **bm.GMT_STYLE_LINE)
    dots = bm.scatter(xs, ys, **bm.GMT_STYLE_DOTS)
    return (line, dots)


def main(argv):
    LOG.info('Creating basemap')

    proj = 'robin'
    if proj == 'mercator':
        lon0 = 25
        bm = ETOPOBasemap(projection='merc',
                   llcrnrlat=-80, llcrnrlon=lon0,
                   urcrnrlat=89, urcrnrlon=lon0 + 360)
        bm.draw_etopo(1, None)
    elif proj == 'robin':
        bm = ETOPOBasemap(projection='robin', lon_0=180)
        bm.draw_etopo(1, None)

    plt.savefig(
        'etopo.png',
        dpi=preset_dpi('480'), format='png',
        transparent=True, bbox_inches='tight', pad_inches=0.1)

    return 0


if __name__ == '__main__':
	sys.exit(main(sys.argv))
