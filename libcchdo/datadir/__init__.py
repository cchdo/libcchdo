"""Operations on the CCHDO data file directories.

"""


def create_expocode(nodc_ship_code, port_departure_date):
    """Generate an ExpoCode from an NODC ship code and port departure date.

    """
    return '{ship_code}{date}'.format(
        ship_code=nodc_ship_code, date=port_departure_date.strftime('%Y%m%d'))
