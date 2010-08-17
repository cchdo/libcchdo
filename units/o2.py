# Ported from
# $Id: o2.c,v 1.4 2004/03/17 20:13:52 frank Exp $
# o2.c 1.3    Solaris 2.4 Unix    950830  SIO/ODF fmd
#
# References:
# ==========
#
# Weast, R.C., Editor, 1971, "CRC Handbook of Chemistry and Physics",
# The Chemical Rubber Co., 1971.
#
# Weiss, R. F., 1970, The solubility of nitrogen, oxygen and argon
# in water and seawater. Deep-Sea Research 17, 721-735.
#
# Dymond, J.H. and Smith, E.B., The Virial Coefficients of Pure Gases
# and Mixtures, Oxford, 518 pp., 1980.
#
# Poynting pressure correction from:
#
# Hitchman, M.L., "Measurement of Dissolved Oxygen",
# pg. 30, eqn 2.28,  John Wiley & Sons, Inc., 1978.


import math

import libcchdo
import libcchdo.fns


ABS_ZERO = -273.15


MOLECULAR_WEIGHT_OF_O2 = 31.9988


# Gas constant, X 10 J Kmole-1 K-1
GAS_CONSTANT = 831.432


# density of O2 g/l @ 273.15K
DENSITY_O2 = 1.42905481


def Kelvin(celsius):
    return celsius - ABS_ZERO


def BunsenO2(s, t):
    ''' Calculate the Bunsen absorption coefficient of dissolved O2 
        in seawater @ s,t
        Args:
            s - salinity (PSS 78)
            t - potential temperature (deg C)
        Return:
            Bunsen coeff for O2
    '''
    a = (-58.3877, 85.8079, 23.8439)
    b = (-0.034892, 0.015568, -0.0019387)

    k100 = Kelvin(t) * 0.01
    return exp(a[0] + a[1] / k100 + a[2] * math.log(k100) +
                 s * (b[0] + k100 * (b[1] + b[2] * k100))) * 1000.0


def Poynting(s, t, p):
    ''' Calculate the Poynting correction for pressure to account
        for isothermal variation of fugacity in a potential field.
        Args:
            s - salinity (PSS 78)
            t - potential temperature (deg C)
            p - pressure (decibars)
        Return:
            Poynting correction
    '''
    # double      IESRho()   /* insitu density @ S,T,P */
    # insitu density g/cm**3
    rhostp = IESRho(s, t, p) * 0.001
    return math.exp(MOLECULAR_WEIGHT_OF_O2 * (p / rhostp) / 
                                             (GAS_CONSTANT * Kelvin(t)))

def O2mlPerLTouMPerL(o2mlpl):
    ''' Convert dissolved O2 concentration (ml/l) to micro-moles/l.
        Args:
            o2mlpl - O2 concentration in ml/l
        Return:
            O2 uM/L
    '''
    C = MOLECULAR_WEIGHT_OF_O2 / DENSITY_O2 * 0.001 # ml/M
    return o2mlpl / C


def O2uMPerLTomlPerL(o2umpl):
    ''' Convert dissolved O2 concentration (micro-moles/L) to ml/L.
        Args:
            o2umpl - O2 concentration in uM/L
        Return:
            O2 ml/l
    '''
    C = MOLECULAR_WEIGHT_OF_O2 / DENSITY_O2 * 0.001 # ml/M
    return o2umpl * C


def O2PerLiterToPerKg(o2mlpl, rho):
    ''' Convert dissolved O2 concentration (ml/l) to micro-moles/Kg.
        Args:
            o2mlpl - O2 concentration in ml/l
            rho - density (S,T,0) (Kg/M**3)
        Return:
            O2 uM/Kg
    '''
    C = MOLECULAR_WEIGHT_OF_O2 / DENSITY_O2 * 0.001 # ml/M
    return o2mlpl / (C * rho * 0.001)


def O2PerKgToPerLiter(o2umpkg, rho):
    ''' Convert dissolved O2 concentration (micro-moles/Kg) to ml/l.
        Args:
             o2umpkg - O2 concentration in uM/Kg
             rho - density (S,T,0) (Kg/M**3)
        Return:
             O2 ml/l
    '''
    C = MOLECULAR_WEIGHT_OF_O2 / DENSITY_O2 * 0.001 # ml/M
    return o2umpkg * (C * rho * 0.001)


def O2MllToPartPress(o2mlpl, s, t, p):
    ''' Convert dissolved O2 concentration (ml/l) to partial-pressure.
        Args:
            o2mlpl - oxygen concentration in ml/l
            s - salinity (PSS 78)
            t - potential temperature (deg C)
            p - pressure (decibars)
        Return:
            O2 part-press (atm)
    '''
    return  o2mlpl / BunsenO2(s, t)


def O2PartPressToMll(o2pp, s, t, p):
    ''' Convert dissolved O2 partial-pressure to concentration (ml/l).
        Args:
            o2pp - oxygen part-press (atm)
            s - salinity (PSS 78)
            t - potential temperature (deg C)
            p - pressure (decibars)
        Return:
            O2 ml/l
    '''
    return o2pp * BunsenO2(s, t)


def O2Saturation(t, s):
    ''' Calculate O2 saturation value (solubility) in seawater.
        Args:
            t - potential temperature (deg C)
            s - salinity (PSS 78)
        Return:
            O2 saturation ml/l
    '''
    a = (-173.4292, 249.6339, 143.3483, -21.8492)
    b = (-0.033096, 0.014259, -0.0017000)

    k100 = Kelvin(t) * 0.01
    return (exp(a[0] + a[1] / k100 + a[2] * math.log(k100) + a[3] * k100 + 
        s * (b[0] + k100 * (b[1] + b[2] * k100))))


def O2SaturationPartPress(s, t, p):
    ''' Calculate O2 saturation value partial-pressure in seawater.
        Args:
            s - salinity (PSS 78)
            t - potential temperature (deg C)
            p - pressure (decibars)
        Return:
            O2 sat part-press
    '''
    return O2MllToPartPress(O2Saturation(t, s), s, t, p)
