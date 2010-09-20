import sys

import libcchdo
import libcchdo.db
import libcchdo.db.model as model
import libcchdo.db.model.legacy
import libcchdo.db.model.std


def _find_legacy_by_mnemonic(name, allow_contrived=False):
    if name.startswith('_'):
        return model.std.make_contrived_parameter(name)
    else:
        return model.legacy.find_parameter(name)


            #libcchdo.LOG.warn(('Conversion from legacy to std parameter '
            #                   "failed for '%s': %s") % (name, e))
            #if allow_contrived:
            #    libcchdo.LOG.warn(('Falling back to contrived.'))
            #    return model.std.make_contrived_parameter(name)

def _get_parameter_alias(name):
    return model.std.session().merge(model.std.ParameterAlias(name))


def parameter(legacy):
    if not legacy:
        return None

    # XXX no attr std?
    parameter = model.std.Parameter(legacy.name)
    parameter.full_name = legacy.full_name
    try:
        parameter.format = '%' + legacy.ruby_precision.strip() if \
            legacy.ruby_precision else '%11s'
    except AttributeError:
        parameter.format = '%11s'
    parameter.description = legacy.description or ''

    range = legacy.range.split(',') if legacy.range else [None, None]
    parameter.bound_lower = float(range[0]) if range[0] else None
    parameter.bound_upper = float(range[1]) if range[1] else None

    parameter.units = model.std.Unit(
        legacy.units, legacy.unit_mnemonic) if \
        legacy.units else None
    parameter.mnemonic = legacy.name

    aliases = map(lambda x: x.strip(), legacy.alias.split(',')) if legacy.alias else []
    parameter.aliases = map(_get_parameter_alias, aliases)
        
    try:
        parameter.display_order = legacy.display_order
    except AttributeError:
        parameter.display_order = sys.maxint

    return parameter


def all_parameters():
    legacy_parameters = model.legacy.session().query(model.legacy.Parameter).all()

    std_parameters = map(parameter, legacy_parameters)

    libcchdo.LOG.info(str(map(lambda x: x.name, std_parameters)))
# TODO do conversion

    # Additional modifications
    # Add EXPOCODE and SECT_ID to known parameters
    display_order = 1
    std_parameters.insert(0, model.std.Parameter(
        'EXPOCODE', 'ExpoCode', '%11s', display_order=display_order))
    display_order += 1
    std_parameters.insert(1, model.std.Parameter(
        'SECT_ID', 'Section ID', '%11s', display_order=display_order))
    display_order += 1
    # Change CTDOXY's precision to 9.4f
    i = [p.mnemonic_woce() for p in std_parameters].index('CTDOXY')
    std_parameters[i].format = '%9.4f'

    return std_parameters
