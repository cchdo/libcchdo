#!/usr/bin/env python

''' Display the legacy parameter ids for the parameters in a Bottle Exchange
file. 
'''

from __future__ import with_statement
import sys

import implib
import libcchdo.model.datafile
import libcchdo.db.model.legacy
import libcchdo.formats.bottle.exchange as exbot


def get_legacy_parameter(parameter):
    if not parameter:
        return None
    name = parameter.name
    if name != '_DATETIME':
        return libcchdo.db.model.legacy.find_parameter(name)
    return None


def main(argv):
    if len(argv) < 2:
        print 'Usage:', argv[0], '<exbot file>'
        return 1
    
    with open(argv[1], 'r') as in_file:
        file = libcchdo.model.datafile.DataFile()
        exbot.read(file, in_file)
    
        # Get STD parameters from bottle file
        parameters = file.get_property_for_columns(lambda x: x.parameter)
    
        print 'Parameters for the data set are: ', parameters
        print 'Getting legacy parameters'
    
        legacy_parameters = filter(None, map(get_legacy_parameter, parameters))
    
        print map(lambda x: int(x.id) if x.id else None, legacy_parameters)
        print map(lambda x: x.name, legacy_parameters)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
