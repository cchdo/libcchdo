"""Database of NODC ship codes.

NODC ship codes are used in the generation of CCHDO ExpoCodes.

"""


from libcchdo import LOG


# TODO actually interface with NODC resource
# http://www.nodc.noaa.gov/General/NODC-Archive/platformlist.txt


SHIP_CODE_UNKNOWN = 'ZZ99'


_ship_codes = {}


def ship_code(ship_name, raise_on_unknown=True, default_to=None):
    """Return the NODC ship code for the given ship name.

    Arguments:
    ship_name - the ship name to search for
    raise_on_unknown - (optional) whether to raise a ValueError or return the
        NODC unknown ship code value if the ship was not found.
    default_to - instead of raising or returning the ship unknown code, return
        this value. It is best if this value conforms to the NODC ship code
        format of four alphanumeric values.

    """
    # TODO fuzzy search ship names?
    try:
        return _ship_codes[ship_name]
    except KeyError:
        msg = u'Ship name {0!r} does not have known NODC ship code'.format(
            ship_name)
        if default_to is None and raise_on_unknown:
            raise ValueError(msg)
        else:
            LOG.warn(msg)
            if default_to is not None:
                return default_to
            return SHIP_CODE_UNKNOWN


def reverse_lookup(ship_code):
    """Return a list of ship names that map to the given ship_code."""
    names = []
    for name, code in _ship_codes.items():
        if code != ship_code:
            continue
        names.append(name)
    return names


def register_ship_code(code, ship_name):
    if code:
        _ship_codes[ship_name] = code
    else:
        LOG.warn(
            u'Ship name {0!r} needs an NODC ship code defined.'.format(
            ship_name))


def register_ship_codes(codes):
    for args in codes:
        register_ship_code(*args)


register_ship_codes([
    # ships in BATS timeseries
    # TODO find the ship code
    [None, 'R/V Weatherbird I'],
    ['32CW', 'R/V Cape Henlopen'],
    ['32KZ', 'R/V Cape Hatteras'],
    ['320G', 'R/V Weatherbird II'],
    ['33H4', 'R/V Atlantic Explorer'],
    ['32OC', 'R/V Oceanus'],

    # ships in HOT timeseries
    ['33KB', 'R/V Kilo-Moana'],
    ['316N', 'R/V Knorr'],
    ['33KI', 'R/V K-O-K'],
    ['3250', 'R/V Thompson'],
    ['32MW', 'R/V Moana Wave'],
    ['33KA', 'SSP Kaimalino'],
    ['31HX', 'R/V Alpha Helix'],
    ['33KL', 'R/V Kila'],
    ['31WM', 'R/V Cromwell'],
    ['32NM', 'R/V New Horizon'],
    ['33NA', "R/V Na'Ina"],
    ['33NA', "M/V Na'Ina"],
    ['3230', "R/V Maurice Ewing"],
    ['33RR', "R/V Roger Revelle"],
    ['33RR', "R/V Roger-Revelle"],
    ['318M', "R/V Melville"],
    ['32WC', "R/V Wecoma"],
])
