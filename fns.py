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
        datafile = libcchdo.DataFileCollection()
    elif filename.endswith('su.txt'):
        datafile = libcchdo.SummaryFile()
    else:
        datafile = libcchdo.DataFile()

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
