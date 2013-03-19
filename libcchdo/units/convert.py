""" Unit converters """


from __future__ import with_statement
from decimal import localcontext, Decimal

from libcchdo import LOG
from libcchdo.fns import _decimal
from ..algorithms import volume
from ..db.model import std


APPROXIMATION_SALINITY = 34.8
APPROXIMATION_TEMPERATURE = 25.0


def _get_first_value_of_parameters(file, parameters, i):
    for parameter in parameters:
        try:
            return file.columns[parameter][i]
        except KeyError:
            pass
    return None


def equivalent(file, column):
    return column


def oxygen_method_is_whole_not_aliquot():
    whole_or_aliquot = None
    while not whole_or_aliquot or whole_or_aliquot.lower() not in ('w', 'a'):
        whole_or_aliquot = raw_input(('Were bottle oxygens Whole bottle '
                                      'or Aliquot? (W/A): ')).lower().strip()
        if whole_or_aliquot == 'w':
            return True
        elif whole_or_aliquot == 'a':
            LOG.warn('Will use temp=25. for oxygen conversion.')
            return False
        else:
            print 'Please enter W or A.'
            print "In truth it probably doesn't matter."


def milliliter_per_liter_to_umol_per_kg(file, column, whole_not_aliquot=None):
    if whole_not_aliquot is None:
        whole_not_aliquot = oxygen_method_is_whole_not_aliquot()

    for i, value in enumerate(column.values):
        salinity = _get_first_value_of_parameters(
            file, ('CTDSAL', 'SALNTY'), i) or APPROXIMATION_SALINITY

        # Salinity sanity check
        if salinity <= 0:
            salinity = APPROXIMATION_SALINITY
        elif salinity < 20 or salinity > 60:
            LOG.warn('Salinity (%f) is ridiculous' % salinity)

        temperature = _get_first_value_of_parameters(
            file, ('CTDTMP', 'THETA', 'REVTMP'), i)
        temperature_missing = not (temperature and temperature > -3)

        if value < -3:
            # Missing
            column.values[i] = None
        elif 'OXY' in column.parameter.mnemonic_woce():
            # Converting oxygen
            if not whole_not_aliquot and \
               'CTDOXY' in column.parameter.mnemonic_woce():
                temperature = APPROXIMATION_TEMPERATURE
            elif temperature_missing:
                temperature = APPROXIMATION_TEMPERATURE
                LOG.warn(('Temperature is missing. Using %f at '
                                   'record#%d') % (temperature, i))
            sigt = volume.sigma_r(
                0.0, 0.0, temperature, salinity)
            o2_atomic_weight = 31.9988
            density_o2 = 1.42905481 # g/l @ 273.15K
            constant = o2_atomic_weight / density_o2 * 0.001
            column.values[i] /= (
                _decimal(constant) * (sigt / _decimal(1.0e3) + _decimal(1.0)))
        else:
            raise ValueError(('Cannot apply conversion for oxygen to '
                              'non-oxygen parameter.'))

    # Change the units
    if 'OXY' in column.parameter.units.name:
        column.parameter.unit = std.Unit('UMOL/KG')

    return column


def mol_per_liter_to_mol_per_kg(file, column):
    if 'OXY' in column.parameter.mnemonic_woce():
        raise ValueError(('Cannot apply mol/liter to mol/kg converter to '
                          'oxygen.'))
    for i, value in enumerate(column.values):
        salinity = _get_first_value_of_parameters(
            file, ('CTDSAL', 'SALNTY'), i) or APPROXIMATION_SALINITY

        # Salinity sanity check
        if salinity <= 0:
            salinity = APPROXIMATION_SALINITY
        elif salinity < 20 or salinity > 60:
            LOG.warn('Salinity (%f) is ridiculous' % salinity)

        temperature = _get_first_value_of_parameters(
            file, ('CTDTMP', 'THETA', 'REVTMP'), i)
        temperature_missing = not (temperature and temperature > -3)

        if value < -3:
            # Missing
            column.values[i] = None
        else:
            column.values[i] /= (volume.sigma_r(
                                     0.0, 0.0, 25.0, salinity) / 1.0e3 + 1.0)

    # Change the units
    prefix = column.parameter.units.name.strip()[:-1]
    column.parameter.unit = std.Unit(prefix + 'KG')

    return column


def ctdoxy_micromole_per_liter_to_micromole_per_kilogram(file, column):
    sigtheta = file['CTDSIGTH']
    if not sigtheta:
    	LOG.warn('Unable to find sigma theta column. Cannot convert.')
        return column
    for i, value in enumerate(column):
    	precision = len(str(column[i].to_integral())) + \
    	    min(-sigtheta[i].as_tuple().exponent,
    	        -column[i].as_tuple().exponent)
        with localcontext() as ctx:
            factor = sigtheta[i].fma(Decimal('1.0e-3'), 1)
            ctx.prec = precision
            column[i] /= factor
    return column


