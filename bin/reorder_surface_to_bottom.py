#!/usr/bin/env python

'''
Put casts into ascending pressure sequenced order.
'''


from __future__ import with_statement
import operator
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.bottle.exchange as botex


PRESSURE_PARAMETERS = ('CTDPRS', 'CTDRAW', )


def swap_rows(a, b, file):
    for c in file.columns.values():
        c.values[a], c.values[b] = c.values[b], c.values[a]

def sort_file_range(file, start, end, ascending):
    pressure_col = None
    for p in PRESSURE_PARAMETERS:
        try:
            pressure_col = file.columns[p]
        except KeyError:
            pass
    if pressure_col is None:
        return
    pressures_orders = zip(pressure_col.values[start:end], range(start, end))
    order = [x for p, x in pressures_orders]
    sorted_order = [x for p, x in sorted(sorted(pressures_orders,
                                                key=operator.itemgetter(1),
                                                reverse=ascending),
                                         key=operator.itemgetter(0),
                                         reverse=(not ascending))]

    i = 0
    while i < len(pressures_orders) / 2 + 1:
        if order[i] != sorted_order[i]:
            j = sorted_order.index(order[i])
            swap_rows(order[i], order[j], file)
            order[i], order[j] = order[j], order[i]
        else:
            i += 1


def reorder_file_pressure(file, ascending=True):
    if len(file) > 0:
        stations = file.columns['STNNBR'].values
        casts = file.columns['CASTNO'].values
        station = stations[0]
        cast = casts[0]
        last_i = 0
        for i in range(1, len(file)):
            station_i = stations[i]
            cast_i = casts[i]
            if station_i != station or cast_i != cast:
                station = station_i
                cast = cast_i
                sort_file_range(file, last_i, i, ascending)
                last_i = i
        sort_file_range(file, last_i, len(file), ascending)


def main(argv):
    '''Reorder casts.'''

    if len(sys.argv) < 2:
        inputfile = raw_input(('Please give an input Exchange filename '
                               '(hy1.csv): ')).strip()
    else:
        inputfile = sys.argv[1]

    if len(sys.argv) < 3:
        outputfile = raw_input(('Please give an output Exchange filename '
                               '(hy1.csv): ')).strip()
    else:
        outputfile = sys.argv[2]

    file = libcchdo.DataFile()

    with open(inputfile, 'r') as f:
        botex.read(file, f)

    reorder_file_pressure(file, ascending=True)

    with open(outputfile, 'w') as f:
        botex.write(file, f)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
