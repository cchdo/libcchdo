#!/usr/bin/env python

import os
import sys
from gzip import GzipFile
from math import floor, fsum
from urllib2 import urlopen, URLError
from tempfile import SpooledTemporaryFile

import numpy as np
from numpy import ma, arange

from scipy.ndimage.interpolation import map_coordinates

from netCDF4 import Dataset

import matplotlib
matplotlib.use('Agg')
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.pyplot as plt

from mpl_toolkits.basemap import Basemap, shiftgrid, _pseudocyl, _cylproj

from libcchdo import config, LOG
from libcchdo.plot.interpolation import BicubicConvolution


etopo_root = 'http://www.ngdc.noaa.gov/mgg/global'


etopo1_root = etopo_root + '/relief/ETOPO1/data'


etopo_dir = os.path.join(config.get_config_dir(), 'etopos')


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
    elif self.projection in _pseudocyl:
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

    if force_resample or not os.path.isfile(etopo_path):
        if arcmins is 1:
            download_etopo1(etopo_path, version)
            data = read_etopo(etopo_path)
        else:
            data = read_etopo(
                os.path.join(etopo_dir, etopo_filename(version=version)))
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


def colormap_cberys(topo, etopo_offset=0):
    """ Gives a colormap based on the topo range and offset that is based off
        an original color scheme created by Carolina Berys.

    """
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

    locolor = (0.173, 0.263, 0.898)
    locolor = (0.130, 0.220, 0.855)
    hicolor = (0.549019607843137, 0.627450980392157, 1)
    hicolor = (0.509019607843137, 0.587450980392157, 1)
    ground_color = (0, 0, 0)
    mount_color = (0.1, 0.1, 0.1)

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


def create_map(etopo_scale, cut, cmtopo=colormap_cberys, version='ice',
               force_resample=False, *args, **kwargs):
    """ Create a basemap with an etopo bathymetry overlay

        Arguments:
            etopo_scale - the scale of the etopo data
            cut - the ratio of points to cut out. The higher this is the more
                averaging of the etopo will occur. Values over 10 will generally
                result in a poor map and values under 3 will result in a very
                long transform time.

    """
    topo, lons, lats, etopo_offset = etopo(etopo_scale, version, force_resample)

    LOG.info('Creating basemap')
    m = Basemap(*args, **kwargs)
    plt.axis('off')

    if cut is None:
        if etopo_scale == 1:
            cut = 6
        else:
            cut = 4

    LOG.debug('Cut ratio %d' % cut)
    nx, ny = get_nx_ny(m, lons, cut_ratio=cut)
    LOG.debug('nx: %d, ny: %d' % (nx, ny))

    if lons.min() < m.llcrnrlon and m.projection in _cylproj:
        LOG.info('Shifting grid')
        topo, lons = shiftgrid(m.llcrnrlon, topo, lons)

    LOG.info('Transforming grid')
    #LOG.debug('lons: %s lats: %s nx: %d ny: %d' % (lons, lats, nx, ny))
    topo, tx, ty = m.transform_scalar(topo, lons, lats, nx, ny, returnxy=True)

    LOG.info('Masking projection bounds')
    topo = mask_proj_bounds(m, topo, lons, lats, nx, ny, tx, ty)

    m.imshow(topo, cmap=cmtopo(topo, etopo_offset))
    return m


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


def main(argv):
    proj = 'robin'
    if proj == 'mercator':
        lon0 = 25
        m = create_map(1, None, projection='merc',
                       llcrnrlat=-80, llcrnrlon=lon0,
                       urcrnrlat=89, urcrnrlon=lon0 + 360)
    elif proj == 'robin':
        m = create_map(1, None, projection='robin', lon_0=180)

    plt.savefig(
        'etopo.png',
        dpi=preset_dpi('480'), format='png',
        transparent=True, bbox_inches='tight', pad_inches=0)

    return 0


if __name__ == '__main__':
	sys.exit(main(sys.argv))
