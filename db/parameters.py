""" libcchdo.db.parameters
Ways to get parameters from databases.
"""


import sys

import connect
import model
import model.legacy
import model.convert


def find_by_mnemonic(name, contrived=False):
    if contrived or name.startswith('_'):
        parameter = model.std.Parameter(name)
        parameter.full_name = name
        parameter.format = '%11s'
        parameter.units = None
        parameter.bound_lower = None
        parameter.bound_upper = None
        parameter.units = None
        parameter.display_order = sys.maxint
        return parameter
    else:
        # Get a legacy parameter and do some conversions
        legacy = model.legacy
        legacy_parameter = legacy.session().query(legacy.Parameter).filter(
            legacy.Parameter.name == name).first()

        if not legacy_parameter:
            legacy_parameter = legacy.Parameter.find_known(name)
        else:
            try:
                legacy_parameter.display_order = \
                    model.legacy.MYSQL_PARAMETER_DISPLAY_ORDERS[
                        legacy_parameter.name]
            except:
                self.display_order = sys.maxint

        if legacy_parameter.name == 'CTDNOBS':
            print legacy_parameter.__dict__

        parameter = model.convert.parameter(legacy_parameter)
        if legacy_parameter.name == 'CTDNOBS':
            print parameter.__dict__

        # std parameter
        #return model.std.session().query(model.std.Parameter).filter(
        #    model.std.Parameter.name == name).first()

        return parameter

