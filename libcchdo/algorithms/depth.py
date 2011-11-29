from decimal import Decimal

from .. import fns


polyn = fns.polynomial


def _decimalify(l):
    return map(fns._decimal, l)


def grav_ocean_surface_wrt_latitude(latitude):
    return Decimal('9.780318') * (Decimal(1) + \
        Decimal('5.2788e-3') * (fns.sin(latitude) ** Decimal(2)) + \
        Decimal('2.35e-5') * (fns.sin(latitude) ** Decimal(4)))


# Following two functions ports of
# $Id: depth.c,v 11589a696ce7 2008/10/15 22:56:57 fdelahoyde $
# depth.c	1.1	Solaris 2.3 Unix	940906	SIO/ODF	fmd


# Correction for gravity as pressure increases 
# (closer to center of Earth)
DGRAV_DPRES = Decimal('2.184e-6')


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
    p = _decimalify(p)
    rho = _decimalify(rho)

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
            depth.append(Decimal(0))
        else:
            depth.append(p[0] / (rho[0] * Decimal(10000) * \
                                 (grav + DGRAV_DPRES * p[0])))

    # Calculate the rest of the series.
    for i in range(0, num_intervals - 1):
        j = i + 1
        # depth in meters
        depth.insert(j, depth[i] + \
                        (p[j] - p[i]) / ((rho[j] + rho[i]) * Decimal(5000) * \
                        (grav + DGRAV_DPRES * p[j])) * Decimal('1e8'))

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
    t = temperature

    if pressure == 0:
        E = _decimalify(('19652.21', '148.4206', '-2.327105',
                         '1.360477e-2', '-5.155288e-5'))
        Kw = polyn(t, E)
        F = _decimalify(('54.6746', '-0.603459', '1.09987e-2', '-6.1670e-5'))
        G = _decimalify(('7.944e-2', '1.6483e-2', '-5.3009e-4'))
        return Kw + polyn(t, F) * salinity + \
               polyn(t, G) * Decimal(float(salinity) ** (3 / 2))

    H = _decimalify(('3.239908', '1.43713e-3', '1.16092e-4', '-5.77905e-7'))
    Aw = polyn(t, H)
    I = _decimalify(('2.2838e-3', '-1.0981e-5', '-1.6078e-6'))
    j0 = Decimal('1.91075e-4')
    A = Aw + polyn(t, I) * salinity + \
        j0 * Decimal(float(salinity) ** (3 / 2))

    K = _decimalify(('8.50935e-5', '-6.12293e-6', '5.2787e-8'))
    Bw = polyn(t, K)
    M = _decimalify(('-9.9348e-7', '2.0816e-8', '9.1697e-10'))
    B = Bw + polyn(t, M) * salinity
    return polyn(pressure,
                      (secant_bulk_modulus(salinity, temperature, 0), A, B))


def density(salinity, temperature, pressure):
    if any(map(lambda x: x is None, (salinity, temperature, pressure))):
        return None

    t = Decimal(str(temperature))

    if pressure == 0:
        A = _decimalify(('999.842594', '6.793952e-2', '-9.095290e-3',
                          '1.001685e-4', '-1.120083e-6', '6.536332e-9'))
        pw = polyn(t, A)
        B = _decimalify(('8.24493e-1', '-4.0899e-3', '7.6438e-5',
                          '-8.2467e-7', '5.3875e-9'))
        C = _decimalify(('-5.72466e-3', '1.0227e-4', '-1.6546e-6'))
        d0 = Decimal('4.8314e-4')

        return pw + polyn(t, B) * salinity + \
               polyn(t, C) * Decimal(float(salinity) ** (3 / 2)) + \
               d0 * (salinity ** Decimal(2))

    # Strange correction of one order of magnitude needed?
    pressure /= Decimal('10')
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
    if not pres or not lat:
        return None
    x = fns.sin(lat / Decimal('57.29578')) ** Decimal(2)
    gr = Decimal('9.780318') * \
        (Decimal(1) + (Decimal('5.2788e-3') + Decimal('2.36e-5') * x) * x) + \
        Decimal('1.092e-6') * pres
    return ((((Decimal('-1.82e-15') * pres + Decimal('2.279e-10')) * pres - \
        Decimal('2.2512e-5')) * pres + Decimal('9.72659')) * pres) / gr
