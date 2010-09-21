import sys
import re

import libcchdo
import libcchdo.db
import libcchdo.db.model as model
import libcchdo.db.model.legacy
import libcchdo.db.model.std


def _get_parameter_alias(session, name):
    return model.std.session().merge(model.std.ParameterAlias(name))


UNITS_MAP = {
    (u'nmol/liter', u'NMOL/L'): (u'nmol/l', u'NMOL/L'),
    (u'umol/kg', u'UMOL/KG'): (u'\u03BCmol/kg', u'UMOL/KG'),
    (u'pmol/liter', u'PMOL/L'): (u'pmol/l', u'PMOL/L'),
}


KNOWN_NETCDF_VARIABLE_NAMES = {
    'OXYGENL': 'oxygenl',
    'OXYGEN': 'bottle_oxygen',
    'CTDOXY': 'oxygen',
    'NITRATL': 'nitratel',
    'NITRAT': 'nitrate',
    'NITRITL': 'nitritel',
    'NITRIT': 'nitrite',
    'CFC-11L': 'freon_11l',
    'CFC-11': 'freon_11',
    'CFC-11L': 'freon_11l',
    'CFC-11': 'freon_11',
    'ALKALI': 'alkalinity',
    'CFC113': 'freon_113',
    'TCARBN': 'total_carbon',
    'CFC-12': 'freon_12',
    'CFC-12L': 'freon_12l',
    'THETA': 'theta',
    'DELHE3': 'delta_helium_3',
    'CTDRAW': 'ctd_raw',
    'PCO2': 'partial_pressure_of_co2',
    'PCO2TMP': 'partial_co2_temperature',
}


def convert_unit(session, name, mnemonic):
    units_name = name.strip()
    units_mnemonic = mnemonic.strip()

    try:
        units_name, units_mnemonic = UNITS_MAP[(units_name, units_mnemonic)]
    except KeyError:
        pass

    units = session.query(model.std.Unit).filter(
         model.std.Unit.name == units_name and \
         model.std.Unit.mnemonic == units_mnemonic).first()
    if not units:
        units = model.std.Unit(units_name, units_mnemonic)
        session.add(units)
    return units


def convert_parameter(session, legacy):
    if not legacy:
        return None

    parameter = model.std.Parameter(legacy.name)
    parameter.full_name = (legacy.full_name or '').strip()
    try:
        parameter.format = '%' + legacy.ruby_precision.strip() if \
            legacy.ruby_precision else '%11s'
    except AttributeError:
        parameter.format = '%11s'
    parameter.description = legacy.description or ''

    range = legacy.range.split(',') if legacy.range else [None, None]
    parameter.bound_lower = float(range[0]) if range[0] else None
    parameter.bound_upper = float(range[1]) if range[1] else None

    if legacy.units:
        parameter.units = convert_unit(session, legacy.units,
                                       legacy.unit_mnemonic)
    else:
        parameter.units = None

    parameter.mnemonic = legacy.name

    aliases = map(lambda x: x.strip(), legacy.alias.split(',')) if \
        legacy.alias else []
    parameter.aliases = map(lambda x: _get_parameter_alias(session, x),
                            aliases)

    try:
        parameter.display_order = legacy.display_order
    except AttributeError:
        parameter.display_order = sys.maxint

    return parameter


_non_word = re.compile('\W+')


def _name_to_netcdf_name(n):
    return _non_word.sub('_', n)


def all_parameters(session):
    legacy_parameters = [model.legacy.find_parameter(x[0]) for x in \
                         model.legacy.session().query(
                             model.legacy.Parameter.name).all()]

    std_parameters = map(lambda x: convert_parameter(session, x),
                         legacy_parameters)
    std_parameters = dict([(x.name, x) for x in std_parameters])

    # Additional modifications
    # Add EXPOCODE and SECT_ID to known parameters
    display_order = 1
    std_parameters['EXPOCODE'] = model.std.Parameter(
        'EXPOCODE', 'ExpoCode', '%11s', display_order=display_order)
    display_order += 1
    std_parameters['SECT_ID'] = model.std.Parameter(
        'SECT_ID', 'Section ID', '%11s', display_order=display_order)
    display_order += 1

    # Change CTDOXY's precision to 9.4f
    std_parameters['CTDOXY'].format = '%9.4f'

    used_netcdf_names = set()

    for p in std_parameters.values():
        if p.name in KNOWN_NETCDF_VARIABLE_NAMES:
            netcdf_name = KNOWN_NETCDF_VARIABLE_NAMES[p.name]
        else:
            best_name = (p.full_name or p.name).lower()
            netcdf_name = _name_to_netcdf_name(best_name)
        while netcdf_name in used_netcdf_names:
            netcdf_name += '1'
        p.name_netcdf = netcdf_name
        used_netcdf_names.add(netcdf_name)

    return std_parameters.values()
