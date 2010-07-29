#!/usr/bin/env python
''' 
A small percentage of WOCE format hydro data is submitted with oxygens
(both bottle and CTD) in ML/L and with nutrients in UMOL/L.  This program
will detect and convert data in /L units and convert them to /KG units.
 
Program asks for:    input filename   (.HYD or .SEA file)
                     output filename:
                     if bottle oxygens:
                           were oxygens whole bottle or aliquot 
 
notes:
    Input format is assumed to be correctly formatted WHP data
    Data columns identifiers and units strings must be correct
      and in upper case.  ie.  oxygens are ML/L
    Oxygen conversion uses sigma T for density. T is set at
      25 C for aliquot oxygens and when T is missing.  
'''


from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))


import libcchdo
import libcchdo.formats.bottle.exchange as botex
import libcchdo.algorithms.volume


APPROXIMATION_SALINITY = 34.8
APPROXIMATION_TEMPERATURE = 25.0


def get_first_value_of_parameters(file, parameters, i):
    for parameter in parameters:
        try:
            return file.columns['parameters'][i]
        except KeyError:
            pass
    return None


def unit_converter_ll_umol_kg_maker(whole_not_aliquot=False):
    def unit_converter_ll_umol_kg(file, column, whole_not_aliquot=False):
        for i, value in enumerate(column.values):
            salinity = get_first_value_of_parameters(
                file, ('CTDSAL', 'SALNTY'), i) or APPROXIMATION_SALINITY

            # Salinity sanity check
            if salinity < 0:
                salinity = APPROXIMATION_SALINITY
            elif 20 < salinity or salinity > 60:
                libcchdo.warn('Salinity (%f) is ridiculous' % salinity)

            temperature = get_first_value_of_parameters(
                file, ('CTDTMP', 'THETA', 'REVTMP'), i)
            temperature_missing = not (temperature and temperature > -3)

            if value < -3:
                # Missing
                column.values[i] = None
            elif 'OXY' in column.parameter.mnemonic_woce():
                # Converting oxygen
                if not whole_not_aliquot and not \
                   'CTDOXY' in column.parameter.mnemonic_woce():
                    temperature = APPROXIMATION_TEMPERATURE
                elif temperature_missing:
                    temperature = APPROXIMATION_TEMPERATURE
                    libcchdo.warn(('Temperature is missing. Using %f at '
                                   'record#%d') % (temperature, i))
                sigt = libcchdo.algorithms.volume.sigma_p(
                    0, 0, temperature, salinity)
                column.values[i] /= (0.022392 * (sigt / 1e3 + 1.0))

            else:
                # Everything not oxygen
                pdensity = libcchdo.algorithms.volume.sigma_p(
                    0, 0, 25.0, salinity)
                column.values[i] /= (pdensity / 1e3 + 1.0)

        # Change the units TODO
        # if unit has 'OXY':
            # unit = 'UMOL/KG'
        # else:
            # unit = '{U,P,N}MOL/KG'

        return column

    return lambda file, column: unit_converter_ll_umol_kg(
                                    file, column, whole_not_aliquot)


def main():
    '''Converts WOCE format L/L units to /KG.'''
    if len(sys.argv) < 2:
        print 'enter input (.sea) filename:'
        filename = sys.stdin.readline().strip()
    else:
        filename = sys.argv[1]

    file = libcchdo.DataFile()

    with open(filename, 'r') as f:
        botex.read(file, f)

    oxygen_present = False
    for column in file.columns.keys():
        if 'OXY' in column:
            oxygen_present = True
            break

    whole_not_aliquot = False
    if oxygen_present:
        # Ask about oxygen method
        print 'Were bottle oxygens Whole bottle or Aliquot? (W/A): ',
        while True:
            whole_or_aliquot = sys.stdin.readline().strip().upper()
            if whole_or_aliquot == 'W':
                whole_not_aliquot = True
                break
            elif whole_or_aliquot == 'A':
                whole_not_aliquot = False
                print 'Will use temp=25. for oxygen conversion.'
                break
            else:
                print 'Please enter W or A.'
                print "In truth it probably doesn't matter: ",

    file.unit_converters[('L/L', u'UMOL/KG')] = \
        unit_converter_ll_umol_kg_maker(whole_not_aliquot)
    file.check_and_replace_parameters()

    botex.write(file, sys.stdout)

if __name__ == '__main__':
    sys.exit(main())
