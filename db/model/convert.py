import sys

import libcchdo.db.model.std as std

def parameter(legacy):
    if legacy:
        parameter = std.Parameter(legacy.name)
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

        parameter.units = std.Unit(
            legacy.units, legacy.unit_mnemonic) if \
            legacy.units else None
        parameter.mnemonic = legacy.name

        def find_or_create_parameter_alias(name):
            alias = std.session().query(std.ParameterAlias).filter(
                std.ParameterAlias.name==name).first()

            if not alias:
                alias = std.ParameterAlias(name)

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


