""" Functions used globally by libcchdo """


import math
import os.path

import libcchdo


def uniquify(seq):
    '''Order preserving uniquify.
       http://www.peterbe.com/plog/uniqifiers-benchmark/
         uniqifiers_benchmark.py (f8 by Dave Kirby)
    '''
    seen = set()
    a = seen.add
    return [x for x in seq if x not in seen and not a(x)]


def strip_all(list):
    return map(lambda x: x.strip(), list)


def read_arbitrary(handle):
    '''Takes any CCHDO recognized file and tries to open it.
       The recognition is done by file extension.
       Args:
           handle - a file handle
       Returns:
           a DataFile(Collection) or *SummaryFile that matches the file type.
    '''
    filename = handle.name
    if not os.path.exists(filename):
        raise ValueError("The file '%s' does not exist" % filename)

    if filename.endswith('zip'):
        datafile = DataFileCollection()
    elif filename.endswith('su.txt'):
        datafile = SummaryFile()
    else:
        datafile = DataFile()

    if filename.endswith('su.txt'):
        import formats.summary.woce
        formats.summary.woce.read(datafile, handle)
    elif filename.endswith('hy.txt'):
        import formats.bottle.woce
        formats.bottle.woce.read(datafile, handle)
    elif filename.endswith('hy1.csv'):
        import formats.bottle.exchange
        formats.bottle.exchange.read(datafile, handle)
    elif filename.endswith('hy1.nc'):
        import formats.bottle.netcdf
        formats.bottle.netcdf.read(datafile, handle)
    elif filename.endswith('nc_hyd.zip'):
        import formats.bottle.zip.netcdf
        formats.bottle.zip.netcdf.read(datafile, handle)
    elif filename.endswith('ct.zip'):
        import formats.bottle.zip.woce
        formats.ctd.zip.woce.read(datafile, handle)
    elif filename.endswith('ct1.zip'):
        import formats.ctd.zip.exchange
        formats.ctd.zip.exchange.read(datafile, handle)
    elif filename.endswith('ctd.nc'):
        import formats.ctd.netcdf
        formats.ctd.netcdf.read(datafile, handle)
    elif filename.endswith('nc_ctd.zip'):
        import formats.ctd.zip.netcdf
        formats.ctd.zip.netcdf.read(datafile, handle)
    else:
      raise ValueError('Unrecognized file type for %s' % filename)

    return datafile


def great_circle_distance(lat_stand, lng_stand, lat_fore, lng_fore):
    delta_lng = lng_fore - lng_stand
    cos_lat_fore = math.cos(lat_fore)
    cos_lat_stand = math.cos(lat_stand)
    cos_lat_fore_cos_delta_lng = cos_lat_fore * math.cos(delta_lng)
    sin_lat_stand = math.sin(lat_stand)
    sin_lat_fore = math.sin(lat_fore)

    # Vicenty formula from Wikipedia
    # fraction_top = sqrt( (cos_lat_fore * sin(delta_lng)) ** 2 +
    #                      (cos_lat_stand * sin_lat_fore -
    #                       sin_lat_stand * cos_lat_fore_cos_delta_lng) ** 2)
    # fraction_bottom = sin_lat_stand * sin_lat_fore +
    #                   cos_lat_stand * cos_lat_fore_cos_delta_lng
    # central_angle = atan2(1.0, fraction_top/fraction_bottom)

    # simple formula from wikipedia
    central_angle = math.acos(cos_lat_stand * cos_lat_fore * \
                              math.cos(delta_lng) + \
                              sin_lat_stand * sin_lat_fore)

    arc_length = libcchdo.RADIUS_EARTH * central_angle
    return arc_length


def strftime_iso(dtime):
    return dtime.isoformat()+'Z'


def equal_with_epsilon(a, b, epsilon=1e-6):
    delta = abs(a - b)
    return delta < epsilon


def out_of_band(value, oob=-999, tolerance=0.1):
    try:
        number = float(value)
    except (ValueError):
        return False
    except TypeError:
        return True
    return equal_with_epsilon(oob, number, tolerance)


def in_band_or_none(x, oob=None, tolerance=None):
    """In band or none
       Args:
           x - anything
           oob - out-of-band value (defaults to out_of_band's default)
           tolerance - out-of-band tolerance (defaults to out_of_band's
                                              default)
       Returns:
           x or None if x is out of band
    """
    args = [x]
    if oob:
        args.append(oob)
    if tolerance:
        args.append(tolerance)
    return None if out_of_band(*args) else x


def identity_or_oob(x, oob=-999):
    """Identity or OOB (XXX)
       Args:
           x - anything
           oob - out-of-band value (default -999)
       Returns:
           identity or out-of-band value.
    """
    return x if x else oob


def grav_ocean_surface_wrt_latitude(latitude):
    return 9.780318 * (1.0 + 5.2788e-3 * math.sin(latitude) ** 2 +
                             2.35e-5 * math.sin(latitude) ** 4)


# Following two functions ports of
# $Id: depth.c,v 11589a696ce7 2008/10/15 22:56:57 fdelahoyde $
# depth.c	1.1	Solaris 2.3 Unix	940906	SIO/ODF	fmd

DGRAV_DPRES = 2.184e-6 # Correction for gravity as pressure increases (closer
                       # to center of Earth


def depth(grav, p, rho):
    """Calculate depth by integration of insitu density.

    Sverdrup, H. U.,Johnson, M. W., and Fleming, R. H., 1942.
    The Oceans, Their Physics, Chemistry and General Biology.
    Prentice-Hall, Inc., Englewood Cliff, N.J.

    Args:
        grav: local gravity (m/sec^2) @ 0.0 db
        p: pressure series (decibars)
        rho: insitu density series (kg/m^3)

    Returns:
        depth - depth series (meters)
    """
    depth = []

    num_intervals = len(p)
    if not (num_intervals == len(rho)):
        raise ValueError("The number of series intervals must be the same.")

    # When calling depth() repeatedly with a two-element
    # series, the first call should be with a one-element series to
    # initialize the starting value (see depth_(), below).

    # Initialize the series
    if num_intervals is not 2:
        # If the integration starts from > 15 db, calculate depth relative to
        # starting place. Otherwise, calculate from surface.
        if p[0] > 15.0:
            depth.append(0.0)
        else:
            depth.append(p[0] / (rho[0] * 10000.0 * \
                                 (grav + DGRAV_DPRES * p[0])))

    # Calculate the rest of the series.
    for i in range(0, num_intervals - 1):
        j = i + 1
        # depth in meters
        depth.insert(j, depth[i] + (p[j] - p[i]) / \
                                   ((rho[j] + rho[i]) * 5000.0 * \
                                    (grav + DGRAV_DPRES * p[j])) * 1e8)

    return depth


def polynomial(x, coeffs):
    """Calculate a polynomial.
    
    Gives the result of calculating
    coeffs[n]*x**n + coeffs[n-1]*x**n-1 + ... + coeffs[0]
    """
    if len(coeffs) <= 0:
        return 0
    sum = coeffs[0]
    degreed = x
    for coef in coeffs[1:]:
        sum += coef * degreed
        degreed *= x
    return sum


def secant_bulk_modulus(salinity, temperature, pressure):
    """Calculate the secant bulk modulus of sea water.
    
    Obtained from EOS80 according to Fofonoff Millard 1983 pg 15

    Args:
        salinity: [PSS-78]
        temperature: [degrees Celsius IPTS-68]
        pressure: pressure

    Returns:
        The secant bulk modulus of sea water as a float.
    """
    t = temperature

    if pressure == 0:
        E = (19652.21, 148.4206, -2.327105, 1.360477e-2, -5.155288e-5)
        Kw = polynomial(t, E)
        F = (54.6746, -0.603459, 1.09987e-2, -6.1670e-5)
        G = (7.944e-2, 1.6483e-2, -5.3009e-4)
        return Kw + polynomial(t, F) * salinity + \
               polynomial(t, G) * salinity ** (3.0 / 2.0)
    H = (3.239908, 1.43713e-3, 1.16092e-4, -5.77905e-7)
    Aw = polynomial(t, H)
    I = (2.2838e-3, -1.0981e-5, -1.6078e-6)
    j0 = 1.91075e-4
    A = Aw + polynomial(t, I) * salinity + j0 * salinity ** (3.0 / 2.0)

    K = (8.50935e-5, -6.12293e-6, 5.2787e-8)
    Bw = polynomial(t, K)
    M = (-9.9348e-7, 2.0816e-8, 9.1697e-10)
    B = Bw + polynomial(t, M) * salinity
    return polynomial(pressure,
                      (secant_bulk_modulus(salinity, temperature, 0), A, B))


def density(salinity, temperature, pressure):
    if any(map(lambda x: x is None, (salinity, temperature, pressure))):
        return None

    t = float(temperature)

    if pressure == 0:
        A = (999.842594, 6.793952e-2, -9.095290e-3,
             1.001685e-4, -1.120083e-6, 6.536332e-9)
        pw = polynomial(t, A)
        B = (8.24493e-1, -4.0899e-3, 7.6438e-5, -8.2467e-7, 5.3875e-9)
        C = (-5.72466e-3, 1.0227e-4, -1.6546e-6)
        d0 = 4.8314e-4
        return pw + polynomial(t, B) * salinity + \
               polynomial(t, C) * salinity ** (3.0 / 2.0) + d0 * salinity ** 2
    pressure /= 10 # Strange correction of one order of magnitude needed?
    return density(salinity, t, 0) / \
           (1 - (pressure / secant_bulk_modulus(salinity, t, pressure)))


def depth_unesco(pres, lat):
    """Depth (meters) from pressure (decibars) using
    Saunders and Fofonoff's method.

    Saunders, P. M., 1981. Practical Conversion of Pressure to Depth.
    Journal of Physical Oceanography 11, 573-574.
    Mantyla, A. W., 1982-1983. Private correspondence.

    Deep-sea Res., 1976, 23, 109-111.
    Formula refitted for 1980 equation of state
    Ported from Unesco 1983
    Units:
      pressure  p     decibars
      latitude  lat   degrees
      depth     depth meters
    Checkvalue: depth = 9712.653 M for P=10000 decibars,
                latitude=30 deg above
      for standard ocean: T=0 deg celsius; S=35 (PSS-78)
    """

    x = math.sin(lat / 57.29578) ** 2
    gr = 9.780318 * (1.0 + (5.2788e-3 + 2.36e-5 * x) * x) + 1.092e-6 * pres
    return ((((-1.82e-15 * pres + 2.279e-10) * pres - 2.2512e-5) * \
           pres + 9.72659) * pres) / gr
