import math

import libcchdo.fns


polynomial = libcchdo.fns.polynomial


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
    assert num_intervals == len(rho), \
           "The number of series intervals must be the same."

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
