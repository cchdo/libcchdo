#!/usr/bin/env python

'''
Display's the legacy parameter id's for the parameters in a Bottle Exchange
file. 
'''

from __future__ import with_statement

import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.db.parameters
import libcchdo.formats.bottle.exchange as exbot


def get_legacy_parameter(parameter):
    if not parameter:
        return None
    name = parameter.name
    if name != '_DATETIME':
        return libcchdo.db.parameters.find_legacy_parameter(name)
    return None


if len(sys.argv) < 2:
    print 'Usage:', sys.argv[0], '<exbot file>'
    sys.exit(1)

with open(sys.argv[1], 'r') as in_file:
    file = libcchdo.DataFile()
    exbot.read(file, in_file)

    # Get STD parameters from bottle file
    parameters = file.get_property_for_columns(lambda x: x.parameter)

    print 'Parameters for the data set are: ', parameters
    print 'Getting legacy parameters'

    legacy_parameters = filter(None, map(get_legacy_parameter, parameters))

    print map(lambda x: int(x.id) if x.id else None, legacy_parameters)
    print map(lambda x: x.name, legacy_parameters)
