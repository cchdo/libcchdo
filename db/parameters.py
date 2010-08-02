""" libcchdo.db.parameters
Ways to get parameters from databases.
"""


import sys

import libcchdo
import connect
import model
import model.legacy
import model.convert


def make_contrived_parameter(name, format=None, units=None, bound_lower=None,
                             bound_upper=None, display_order=sys.maxint):
    return model.std.Parameter(
        name,
        full_name=name,
        format=format or '%11s', 
        units=model.std.Unit(units, units) if units else None,
        bound_lower=bound_lower,
        bound_upper=bound_upper,
        display_order=display_order)


def find_legacy_parameter(name):
    legacy = model.legacy
    session = legacy.session()
    legacy_parameter = session.query(legacy.Parameter).filter(
        legacy.Parameter.name == name).first()

    if not legacy_parameter:
        # Try aliases
        legacy_parameter = session.query(legacy.Parameter).filter(
            legacy.Parameter.alias.like('%%%s%%' % name)).first()
        
        if not legacy_parameter:
            # Try known overrides
            libcchdo.warn(("No legacy parameter found for '%s'. Falling back "
                           "on known override parameters.") % name)
            legacy_parameter = legacy.Parameter.find_known(name)
    else:
        try:
            legacy_parameter.display_order = \
                model.legacy.MYSQL_PARAMETER_DISPLAY_ORDERS[
                    legacy_parameter.name]
        except:
            legacy_parameter.display_order = sys.maxint

    return legacy_parameter


def find_by_mnemonic(name, allow_contrived=False):
    if name.startswith('_'):
        return make_contrived_parameter(name)
    else:
        try:
            legacy_parameter = find_legacy_parameter(name)
            # std parameter
            #return model.std.session().query(model.std.Parameter).filter(
            #    model.std.Parameter.name == name).first()
            return model.convert.parameter(legacy_parameter)
        except:
            if allow_contrived:
                libcchdo.warn(('Conversion from legacy to std parameter '
                               'failed. Falling back to contrived.'))
                return make_contrived_parameter(name)

        raise EnvironmentError(('Unknown parameter %s. Contrivance not '
                                'allowed.') % name)


def find_by_mnemonic_std(name):
    parameter = model.std.session().query(model.std.Parameter).filter(
        model.std.Parameter.name == name).first()
    if not parameter:
        parameter = model.std.session().query(model.std.ParameterAlias).filter(
            model.std.ParameterAlias.name == name).first()
        if parameter:
            parameter = parameter.parameter
    return parameter
