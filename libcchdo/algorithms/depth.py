from .. import LOG, fns
from libcchdo.fns import _decimal

try:
    from cdecimal import InvalidOperation
except ImportError:
    from decimal import InvalidOperation


polyn = fns.polynomial


def grav_ocean_surface_wrt_latitude(latitude):
    return _decimal('9.780318') * (_decimal(1) + \
        _decimal('5.2788e-3') * (fns.sin(latitude) ** _decimal(2)) + \
        _decimal('2.35e-5') * (fns.sin(latitude) ** _decimal(4)))


# Following two functions ports of
# $Id: depth.c,v 11589a696ce7 2008/10/15 22:56:57 fdelahoyde $
# depth.c	1.1	Solaris 2.3 Unix	940906	SIO/ODF	fmd


# Correction for gravity as pressure increases 
# (closer to center of Earth)
DGRAV_DPRES = _decimal('2.184e-6')


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
    assert num_intervals == len(rho), \
           "The number of series intervals must be the same."

    grav = fns._decimal(grav)
    p = _decimal(p)
    rho = _decimal(rho)

    # When calling depth() repeatedly with a two-element
    # series, the first call should be with a one-element series to
    # initialize the starting value (see depth_(), below).
    # TODO figure out what this does. The original C version has the caller
    # maintain a depth array that is constantly modified.

    # Initialize the series
    if num_intervals is not 2:
        # If the integration starts from > 15 db, calculate depth relative to
        # starting place. Otherwise, calculate from surface.
        if p[0] > 15.0:
            depth.append(_decimal(0))
        else:
            depth.append(p[0] / (rho[0] * _decimal(10000) * \
                                 (grav + DGRAV_DPRES * p[0])))

    # Calculate the rest of the series.
    for i in range(0, num_intervals - 1):
        j = i + 1
        # depth in meters
        depth.insert(j, depth[i] + \
                        (p[j] - p[i]) / ((rho[j] + rho[i]) * _decimal(5000) * \
                        (grav + DGRAV_DPRES * p[j])) * _decimal('1e8'))

    return depth


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
    s = _decimal(salinity)
    t = _decimal(temperature)
    p = _decimal(pressure)

    if p == 0:
        E = _decimal('19652.21', '148.4206', '-2.327105',
                     '1.360477e-2', '-5.155288e-5')
        Kw = polyn(t, E)
        F = _decimal('54.6746', '-0.603459', '1.09987e-2', '-6.1670e-5')
        G = _decimal('7.944e-2', '1.6483e-2', '-5.3009e-4')
        try:
            return Kw + polyn(t, F) * s + \
                   polyn(t, G) * (s ** 3).sqrt()
        except InvalidOperation, e:
            LOG.debug('Invalid operation probably caused by salinity = %r' % s)
            raise e

    H = _decimal('3.239908', '1.43713e-3', '1.16092e-4', '-5.77905e-7')
    Aw = polyn(t, H)
    I = _decimal('2.2838e-3', '-1.0981e-5', '-1.6078e-6')
    j0 = _decimal('1.91075e-4')
    try:
        A = Aw + polyn(t, I) * s + j0 * (s ** 3).sqrt()
    except InvalidOperation, e:
        LOG.debug('Invalid operation probably caused by salinity = %r' % s)
        raise e

    K = _decimal('8.50935e-5', '-6.12293e-6', '5.2787e-8')
    Bw = polyn(t, K)
    M = _decimal('-9.9348e-7', '2.0816e-8', '9.1697e-10')
    B = Bw + polyn(t, M) * s
    return polyn(p, (secant_bulk_modulus(s, temperature, 0), A, B))


def density(salinity, temperature, pressure):
    """ Calculates density given salinity, temperature, and pressure.

        The algorithm is given on page -15- of UNESCO 44 as equation (7)

    """
    if any(map(lambda x: x is None, (salinity, temperature, pressure))):
        return None

    s = _decimal(salinity)
    t = _decimal(temperature)
    p = _decimal(pressure)

    if p == 0:
        # UNESCO 44 page - 17 -
        A = _decimal('999.842594', '6.793952e-2', '-9.095290e-3',
                     '1.001685e-4', '-1.120083e-6', '6.536332e-9')
        # equation (14)
        pure_water_d = polyn(t, A)
        B = _decimal('8.24493e-1', '-4.0899e-3', '7.6438e-5',
                     '-8.2467e-7', '5.3875e-9')
        C = _decimal('-5.72466e-3', '1.0227e-4', '-1.6546e-6')
        d0 = _decimal('4.8314e-4')

        try:
            return pure_water_d + polyn(t, B) * s + \
                   polyn(t, C) * (s ** 3).sqrt() + \
                   d0 * (s ** _decimal(2))
        except InvalidOperation, e:
            LOG.debug('Invalid operation probably caused by salinity = %r' % s)
            raise e

    # Strange correction of one order of magnitude needed?
    # A correction is provided in
    # http://www.nioz.nl/public/fys/staff/hendrik_van_aken/dictaten/unesco44.pdf
    # page - 15 -
    # stating that it should be bars instead of decibars
    p /= _decimal('10')
    return density(s, t, 0) / \
           (1 - (p / secant_bulk_modulus(s, t, p)))


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
    if not pres or not lat:
        return None
    x = fns.sin(lat / _decimal('57.29578')) ** _decimal(2)
    gr = _decimal('9.780318') * \
        (_decimal(1) + (_decimal('5.2788e-3') + _decimal('2.36e-5') * x) * x) + \
        _decimal('1.092e-6') * pres
    return ((((_decimal('-1.82e-15') * pres + _decimal('2.279e-10')) * pres - \
        _decimal('2.2512e-5')) * pres + _decimal('9.72659')) * pres) / gr
