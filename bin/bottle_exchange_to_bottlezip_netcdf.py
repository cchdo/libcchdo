#!/usr/bin/env python

from __future__ import with_statement

import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.formats.bottle.exchange as botex
import libcchdo.formats.bottle.zip.netcdf as botzipnc


@libcchdo.memoize
def get_parameters(file):
    parameters = file.get_property_for_columns(
        lambda c: c.parameter.mnemonic_woce() if c and c.parameter else '')
    return parameters

def clone_file_structure(file):
    clone = libcchdo.DataFile()

    parameters = get_parameters(file)
    clone.create_columns(parameters)

    clone.globals = file.globals.copy()

    return clone

def split_bottle(file):
    ''' Split apart the bottle exchange file into a data file collection based
        on station cast. Each cast is a new 'file'.
    '''
    coll = libcchdo.DataFileCollection()

    file_parameters = get_parameters(file)

    current_file = clone_file_structure(file)

    expocodes = file['EXPOCODE']
    stations = file['STNNBR']
    casts = file['CASTNO']

    expocode = expocodes[0]
    station = stations[0]
    cast = casts[0]
    for i in range(len(file)):
        # Check if this row is a new measurement location
        if expocodes[i] != expocode or \
           stations[i] != station or \
           casts[i] != cast:
            coll.files.append(current_file)
            current_file = clone_file_structure(file)
        expocode = expocodes[i]
        station = stations[i]
        cast = casts[i]

        # Put the current row in the current file
        for p in file_parameters:
            source_col = file[p]
            value = source_col[i]
            try:
                flag_woce = source_col.flags_woce[i]
            except IndexError:
                flag_woce = None
            try:
                flag_igoss = source_col.flags_igoss[i]
            except IndexError:
                flag_igoss = None
            current_file[p].append(value, flag_woce, flag_igoss)

    coll.files.append(current_file)

    return coll


def main(argv):
    if len(argv) < 2:
        print 'Usage: %s <bottle exchange zip>' % argv[0]
        return 1
    
    incoming = libcchdo.DataFile()
    with open(argv[1], 'r') as in_file:
        botex.read(incoming, in_file)

    botzipnc.write(split_bottle(incoming), sys.stdout)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
