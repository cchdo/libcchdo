'''libcchdo.formats.woce'''


def woce_lat_to_dec_lat(lattoks):
    '''Convert a latitude in WOCE format to decimal.'''
    lat = int(lattoks[0]) + float(lattoks[1]) / 60.0
    if lattoks[2] != 'N':
        lat *= -1
    return lat


def woce_lng_to_dec_lng(lngtoks):
    '''Convert a longitude in WOCE format to decimal.'''
    lng = int(lngtoks[0]) + float(lngtoks[1]) / 60.0
    if lngtoks[2] != 'E':
        lng *= -1
    return lng


def dec_lat_to_woce_lat(lat):
    '''Convert a decimal latitude to WOCE format.'''
    lat_deg = int(lat)
    lat_dec = abs(lat-lat_deg) * 60
    lat_deg = abs(lat_deg)
    lat_hem = 'S'
    if lat > 0:
        lat_hem = 'N'
    return '%2d %05.2f %1s' % (lat_deg, lat_dec, lat_hem)


def dec_lng_to_woce_lng(lng):
    '''Convert a decimal longitude to WOCE format.'''
    lng_deg = int(lng)
    lng_dec = abs(lng-lng_deg) * 60
    lng_deg = abs(lng_deg)
    lng_hem = 'W'
    if lng > 0 :
        lng_hem = 'E'
    return '%3d %05.2f %1s' % (lng_deg, lng_dec, lng_hem)


def strftime_woce_date_time(dtime):
    return (dtime.strftime('%Y%m%d'), dtime.strftime('%H%M'))
