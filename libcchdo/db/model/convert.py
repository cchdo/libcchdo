import sys
import re

from ... import LOG
from ...db.model import legacy
from ...db.model import std


def _get_parameter_alias(session, name):
    return session.merge(std.ParameterAlias(name))


UNITS_MAP = {
    (u'nmol/liter', u'NMOL/L'): (u'nmol/l', 'NMOL/L'),
    (u'umol/kg', u'UMOL/KG'): (u'\u03BCmol/kg', 'UMOL/KG'),
    (u'pmol/liter', u'PMOL/L'): (u'pmol/l', 'PMOL/L'),
    (u'DBAR', u'DBAR'): (u'decibar', 'DBAR'),
    (u'\xc2\xb0C', u'DEG C'): (u'\xc2\xb0C (DEG C)', 'DEG C'),
    (u'\xc2\xb0C(ITS90)', u'ITS-90'): (u'\xc2\xb0C (ITS-90)', 'ITS-90'),
    (u'\xc2\xb0C', u'ITS-90'): (u'\xc2\xb0C (ITS-90)', 'ITS-90'),
    (u'ITS-90', u'ITS-90'): (u'\xc2\xb0C (ITS-90)', 'ITS-90'),
}


KNOWN_NETCDF_VARIABLE_NAMES = {
    'OXYGENL': u'oxygenl',
    'OXYGEN':  u'bottle_oxygen',
    'CTDOXY':  u'oxygen',
    'NITRATL': u'nitratel',
    'NITRAT':  u'nitrate',
    'NITRITL': u'nitritel',
    'NITRIT':  u'nitrite',
    'CFC-11L': u'freon_11l',
    'CFC-11':  u'freon_11',
    'CFC-11L': u'freon_11l',
    'CFC-11':  u'freon_11',
    'ALKALI':  u'alkalinity',
    'CFC113':  u'freon_113',
    'TCARBN':  u'total_carbon',
    'CFC-12':  u'freon_12',
    'CFC-12L': u'freon_12l',
    'THETA':   u'theta',
    'DELHE3':  u'delta_helium_3',
    'CTDRAW':  u'ctd_raw',
    'PCO2':    u'partial_pressure_of_co2',
    'PCO2TMP': u'partial_co2_temperature',
}


def convert_unit(session, name, mnemonic):
    units_name = unicode(name.strip())
    units_mnemonic = str(mnemonic.strip())

    try:
        units_name, units_mnemonic = UNITS_MAP[(units_name, units_mnemonic)]
    except KeyError:
        pass

    units = session.query(std.Unit).filter(std.Unit.name == units_name).first()
    if not units:
        units = std.Unit(units_name, units_mnemonic)
        session.add(units)
    else:
        if units.mnemonic != units_mnemonic:
            LOG.warn(u'Mismatched mnemonic for unit {0}.'.format(units_name))
            if not units.mnemonic:
                units.mnemonic = units_mnemonic
                LOG.info(u'Setting mnemonic for unit {0} to {1}'.format(
                    units_name, units_mnemonic))
    return units


def _find_or_create_parameter(session, name):
    parameter = session.query(std.Parameter).filter(
        std.Parameter.name == name).first()
    if not parameter:
        parameter = std.Parameter(name)
        session.add(parameter)
    return parameter


def convert_parameter(session, legacy_param):
    if not legacy_param:
        return None

    parameter = _find_or_create_parameter(session, legacy_param.name)

    parameter.full_name = (unicode(legacy_param.full_name) or u'').strip()
    try:
        precision = legacy_param.ruby_precision
        if precision:
            parameter.format = '%' + str(precision.strip())
        else:
            parameter.format = '%11s'
    except AttributeError:
        parameter.format = '%11s'
    parameter.description = unicode(legacy_param.description or u'')

    range = legacy_param.range.split(',') if legacy_param.range else [None, None]
    parameter.bound_lower = float(range[0]) if range[0] else None
    parameter.bound_upper = float(range[1]) if range[1] else None

    if legacy_param.units:
        legacy_param.units = legacy_param.units
        parameter.units = convert_unit(
            session, legacy_param.units, legacy_param.unit_mnemonic)
    else:
        parameter.units = None

    aliases = []
    if legacy_param.alias:
        aliases = [x.strip() for x in legacy_param.alias.split(',')]
    parameter.aliases = [_get_parameter_alias(session, x) for x in aliases]

    try:
        parameter.display_order = legacy_param.display_order
    except AttributeError:
        parameter.display_order = sys.maxint

    return parameter


_non_word = re.compile('\W+')


def _name_to_netcdf_name(n):
    return _non_word.sub('_', n.lower())


def all_parameters(lsession, session):
    """Convert all parameters from legacy to std"""
    legacy_parameter_names = [
        x[0] for x in lsession.query(legacy.Parameter.name).all()]
    legacy_parameters = [
        legacy.find_parameter(lsession, x) for x in legacy_parameter_names]

    std_parameters = [convert_parameter(session, x) for x in legacy_parameters]
    std_parameters = dict([(x.name, x) for x in std_parameters])

    # Additional modifications
    # Add EXPOCODE and SECT_ID to known parameters
    display_order = 0
    expocode = std_parameters['EXPOCODE'] = _find_or_create_parameter(
        session, u'EXPOCODE')
    expocode.full_name = u'ExpoCode'
    expocode.format = '%11s'
    expocode.display_order = display_order
    session.add(expocode)
    display_order += 1

    sectid = std_parameters['SECT_ID'] = _find_or_create_parameter(
        session, u'SECT_ID')
    sectid.full_name = u'Section ID'
    sectid.format = '%11s'
    sectid.display_order = display_order
    session.add(sectid)
    display_order += 1

    # Change CTDOXY's precision to 9.4f
    std_parameters['CTDOXY'].format = '%9.4f'

    used_netcdf_names = set()
    for p in std_parameters.values():
        if p.name in KNOWN_NETCDF_VARIABLE_NAMES:
            netcdf_name = KNOWN_NETCDF_VARIABLE_NAMES[p.name]
        else:
            best_name = p.full_name
            if not best_name or best_name == 'None':
                best_name = p.name
            netcdf_name = _name_to_netcdf_name(best_name)
            if netcdf_name == 'none':
                LOG.debug('{0!r} {1!r} {2}'.format(p.full_name, p.name, p))
        while netcdf_name in used_netcdf_names:
            netcdf_name += '1'
        p.name_netcdf = netcdf_name
        used_netcdf_names.add(netcdf_name)

    return std_parameters.values()
