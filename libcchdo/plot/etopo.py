#!/usr/bin/env python

import os
import sys
from gzip import GzipFile
from math import floor, fsum

import numpy as np
from numpy import ma, arange

from netCDF4 import Dataset

import matplotlib
matplotlib.use('Agg')
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.pyplot as plt

from mpl_toolkits.basemap import Basemap, shiftgrid, _pseudocyl, _cylproj

from libcchdo import config, LOG


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


def etopo(scale=1):
    """ Read and normalize etopo data from a netCDF file.

        ETOPO1 can be downloaded from here:
        http://www.ngdc.noaa.gov/mgg/global/global.html

        Please pick from the grid registered files.

        Returns:
            a tuple with (elevation data, lons, lats,
                          offset_from_ground_to_consider_underwater)

    """
    try:
        etopofile = config.get_option('etopo', 'path')
    except Exception, e:
        url_ice = ('http://www.ngdc.noaa.gov/mgg/global/relief/ETOPO1/data/'
                   'ice_surface/grid_registered/netcdf/ETOPO1_Ice_g_gmt4.grd.gz')
        url_bed = ('http://www.ngdc.noaa.gov/mgg/global/relief/ETOPO1/data/'
                   'bedrock/grid_registered/netcdf/ETOPO1_Bed_g_gmt4.grd.gz')
        LOG.error('An ETOPO1 file is required to use etopo.')
        LOG.error((
            'Please download and gunzip either an ice surface or bedrock file '
            "and add the file path to %s like so: \n"
            "[etopo]\n"
            "path = /full/path/to/not/gzipped/file" % config.get_config_path()))
        LOG.error(url_ice)
        LOG.error(url_bed)
        raise e

    if etopofile.endswith('.gz'):
        LOG.error((
            'Please run gunzip on %s first and update %s '
            'accordingly.' % (etopofile, config.get_config_path())))
        raise ValueError('Cannot open gzipped ETOPO1')

    LOG.info('Opening %s' % etopofile)
    toponc = Dataset(etopofile, 'r')

    # offset bumps the level just above sea level to below so as to err on
    # the side of less land shown
    offset = 50.

    lons = toponc.variables['x'][:]
    lats = toponc.variables['y'][:]
    topo = toponc.variables['z'][:]

    # TODO allow for scaling the etopo data down to increase speed

    toponc.close()
    return (topo, lons, lats, offset)


def colormap_cberys(topo, etopo_offset=0):
    """ Gives a colormap based on the topo range and offset that is based off
        an original color scheme created by Carolina Berys.

    """
    maxt = float(np.max(topo))
    mint = float(np.min(topo))

    shift = etopo_offset - mint

    # range of topo goes from ~ -10k to ~8k
    # let -10k be 0 %
    # let 8k = 100%
    # 0 = x%
    # 10k / (8k + 10k) = x%
    groundpt = shift / (maxt + shift)
    LOG.debug('%s %s %s %s %s', mint, etopo_offset, shift, groundpt, maxt)

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


def create_map(etopo_scale, cut, cmtopo=colormap_cberys, *args, **kwargs):
    """ Create a basemap with an etopo bathymetry overlay

        Arguments:
            etopo_scale - the scale of the etopo data
            cut - the ratio of points to cut out. The higher this is the more
                averaging of the etopo will occur. Values over 10 will generally
                result in a poor map and values under 3 will result in a very
                long transform time.

    """
    topo, lons, lats, etopo_offset = etopo(etopo_scale)

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

    topo, lons = topo, lons
    if lons.min() < m.llcrnrlon and m.projection in _cylproj:
        LOG.info('Shifting grid')
        topo, lons = shiftgrid(m.llcrnrlon, topo, lons)

    LOG.info('Transforming grid')
    topo, tx, ty = m.transform_scalar(topo, lons, lats, nx, ny, returnxy=True)

    LOG.info('Masking projection bounds')
    topo = mask_proj_bounds(m, topo, lons, lats, nx, ny, tx, ty)

    m.imshow(topo, cmap=cmtopo(topo))
    return m


def preset_dpi(level='240'):
    """ Gives a dpi estimate for matplotlib.pyplot.saveimg given some height
    bound.
    """
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
    return level


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
