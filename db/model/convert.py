import sys

import libcchdo.db.parameters
import libcchdo.db.model as model
import libcchdo.db.model.legacy
import libcchdo.db.model.std

def parameter(legacy):
    if legacy:
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

        def find_or_create_parameter_alias(name):
            alias = model.std.session().query(model.std.ParameterAlias).filter(
                model.std.ParameterAlias.name==name).first()

            if not alias:
                alias = model.std.ParameterAlias(name)

            return alias

        parameter.aliases = map(
            lambda x: find_or_create_parameter_alias(x.strip()), 
            legacy.alias.split(',')) if legacy.alias else []
        try:
            parameter.display_order = legacy.display_order
        except AttributeError:
            parameter.display_order = sys.maxint

        return parameter
    else:
        return None


def all_parameters():
    legacy_parameters = [x[0] for x in 
        model.legacy.session().query(model.legacy.Parameter.name).all()]

    std_parameters = [libcchdo.db.parameters.find_by_mnemonic(x) for x in 
        legacy_parameters]

    # Additional modifications
    display_order = 1
    std_parameters.insert(0, model.std.Parameter(
        'EXPOCODE', 'ExpoCode', '%11s', display_order=display_order))
    display_order += 1
    std_parameters.insert(1, model.std.Parameter(
        'SECT_ID', 'Section ID', '%11s', display_order=display_order))
    display_order += 1

    return std_parameters
