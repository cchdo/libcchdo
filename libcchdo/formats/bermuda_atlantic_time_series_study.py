from decimal import Decimal

from .. import fns


def deg_min_to_decimal_deg(deg, min):
    return Decimal(deg) + Decimal(min) / Decimal(60)


def combine(bats_file, event_sum_file):
    """Combines the given BATS .dpr file with the Summary event.log file so
       that the DataFile contains most of the information from both.
    """
    # It is pretty much given that the data is CTD.

    lat, lng = bats_file.globals['LATITUDE'], bats_file.globals['LONGITUDE']

    # Find the event log record
    sum_file_i = None
    for i in range(len(event_sum_file)):
        sumlat, sumlng = event_sum_file['LATITUDE'][i], event_sum_file['LONGITUDE'][i]
        epsilon = Decimal('1e-3')
        close_enough = fns.equal_with_epsilon(lat, sumlat, epsilon) and \
                       fns.equal_with_epsilon(lng, sumlng, epsilon)
        if close_enough:
            sum_file_i = i
            break

    if sum_file_i is None:
        LOG.error('Event for BATS data at %f %f not found' % (lat, lng))
        return
    headers = event_sum_file.column_headers()
    row = event_sum_file.row(i)

    info = dict(zip(headers, row))
    bats_file.globals['DEPTH'] = info['DEPTH']
