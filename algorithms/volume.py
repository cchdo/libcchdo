'''
Ported by Matt Shen from Fortran 77 2010-07-27
newvol.f -> volume.py
'''


import libcchdo


CDMISS = 3.2e4
TOLER = 3e-4

COEFF_PRESSURE = (0, 1.0076, 2.3487e-6, -1.2887e-11)
COEFF_RHO_W = (999.842594, 0.06793952, -0.00909529, 1.001685e-4,
               -1.120083e-6, 6.536332e-9)
COEFF_KW_1 = (0, -0.0040899, 7.6438e-5, -8.2467e-7, 5.3875e-9)
COEFF_K_ST0 = (-0.00572466, 1.0227e-4, -1.6546e-6)


equal_with_epsilon = libcchdo.fns.equal_with_epsilon
polynomial = libcchdo.fns.polynomial


def _missing(x):
    return equal_with_epsilon(x, CDMISS)


def _any_missing(*args):
    return any(map(_missing, args))


def rho_w(potential_temperature):
    '''density of pure water kg/m**3'''
    return polynomial(potential_temperature, COEFF_RHO_W)


def kw_1(potential_temperature):
    return polynomial(potential_temperature, COEFF_KW_1)


def kw(potential_temperature, salinity):
    '''Pure water secant bulk modulus?''' # TODO
    return (kw_1(potential_temperature) + 0.824493) * salinity


def k_st0(salinity, potential_temperature):
    return polynomial(potential_temperature, COEFF_K_ST0) * \
           abs(salinity) ** 1.5


def alphy(s, t, p):
    '''Calculate the specific volume as a function of pressure, 
       potential temperature, and salinity.

       Note: temperature used instead of pot. temp. DMN 10/22/84
    '''
    sigstp = sigma_r(p, p, t, s)

    if _missing(sigstp):
        return CDMISS

    return 1.0 / (sigstp * 1e-3 + 1.0)


def spvoly(s, t, p):
    '''Calculate the specific volume anomaly in 10**2 gm/l as a function of 
       salinity in ppt, potential temperature in degrees C and temperature in
       degrees C.

       Note: potential temperature is no longer used. DMN 10/22/84
    '''
    if _any_missing(s, t, p):
        return CDMISS

    return (alphy(s, t, p) - alphy(35.0, 0, p)) * 1e5


def pressure(z):
    '''Calculates pressure from depth.

       Args:
           z - depth
    '''

    if _missing(z):
        return CDMISS

    return polynomial(z, COEFF_PRESSURE)


def potential_temperature(p, t, s, rp):
    '''Calculate potential temperature for an arbitrary reference pressure.
 
       Ref: N.P. Fofonoff
            Deep Sea Research
            in press Nov 1976
 
       Args:
           press-- pressure in decibars
           temp -- temperature in celsius degrees
           s    -- salinity PSS 78
           rp   -- reference pressure in decibars
                   (0.0 for standard potential temperature)
 
       Return:
           potential temperature
    '''
    s1 = s - 35.0
    if _any_missing(s, p, t):
        return CDMISS

    dp = rp - p
    n = int(abs(dp) / 1.0e3) + 1
    dp /= float(n)

    for i in range(1, n):
        for j in range(1, 4):
            r1 = polynomial(t, (-4.6206e-13, 1.8676e-14, -2.1687e-16)) * p
            r2 = polynomial(t, (-1.1351e-10, 2.7759e-12)) * s1
            r3 = polynomial(t, (0, -6.7795e-10, 8.733e-12, -5.4481e-13))
            r4 = (r1 + (r2 + r3 + 1.8741e-8)) * p + \
                 polynomial(t, (1.8932e-6, -4.2393e-8)) * s1
            r5 = r4 + polynomial(
                          t, (3.5803e-5, 8.5258e-6, -6.836e-8, 6.6228e-10))

            x = dp * r5

            if j == 1:
                t = t + 0.5 * x
                q = x
                p += 0.5 * dp
            elif j == 2:
                t += 0.29289322 * (x - q)
                q = 0.58578644 * x + 0.121320344 * q
            elif j == 3:
                t += 1.707106781 * (x - q)
                q = 3.414213562 * x - 4.121320344 * q
                p += 0.5 * dp
            elif j == 4:
                t += (x - 2.0 * q) / 6.0
    return t


def sigma_r(refprs, press, temp, salty):
    '''Calculate density using international equation of state

       From text furnished by J. Gieskes

       Args:
           press  -- pressure in decibars
           temp   -- temperature in celsius degrees
           salty  -- salinity PSS 78
           refprs -- reference pressure
                     refprs = 0. : sigma theta
                     refprs = press: sigma z

       Return:
           kg/m*3 - 1000.0
    '''

    # check for missing data
    if _any_missing(temp, press, salty):
        return CDMISS

    # calculate potential temperature
    if press != refprs:
        potemp = potential_temperature(press, temp, salty, refprs)
    else:
        potemp = temp

    # sigma theta kg/m**3
    sigma = rho_w(potemp) + kw(potemp, salty) + k_st0(salty, potemp) + 4.8314e-4 * salty ** 2

    if equal_with_epsilon(refprs, 0.0):
        return sigma - 1000.0

    # Calculate pressure effect
    #
    #   rho(s,t,0)/(1.0-p/k(s,t,p))
    #

    kst0 = secant_bulk_modulus(abs(salty), potemp, 0)

    # reference pressure in bars
    bars = refprs * 0.1

    # Calculate pressure terms
    terma = polynomial(potemp, (3.239908, 0.00143713, 1.16092e-4, -5.77905e-7)) + \
            polynomial(potemp, (0.0022838, -1.0981e-5, -1.6078e-6)) * salty + \
            1.91075e-4 * abs(salty) ** 1.5

    termb = polynomial(potemp, (8.50935e-5, -6.12293e-6, 5.2787e-8)) + \
            polynomial(potemp, (-9.9348e-7, 2.0816e-8, 9.1697e-10)) * salty

    # Secant bulk modulus k(s,t,p) */
    kstp = polynomial(bars, (kst0, terma, termb))

    return sigma / (1.0 - bars / kstp) - 1000.0


def oxycal(pt, s, o2):
    '''Routine to calculate precent oxygen saturation.
    
       Args:
            -> ptx     potential temperature.
            -> salx    salinity
            -> o2x     observed oxygen  (ml/l)

       Return:
            percent oxygen saturation.
    
       Ref:  Weiss, R.F. (1970) The Solubility of Nitrogen, Oxygen,
                    and Argon in Water and Seawater. Deep-Sea Res., Vol 17,
                    721-735.
    
       Note: two other values can be calculated using returned value.
             o2c == oxygen saturation of parcel of water moved
                    to surface and exposed to the atmosphere for
                    an infinite amount of time.
             o2c = (o2x*100.)/oxypct
             apparent oxygen utilization:  o2x - o2c
    '''
    if _any_missing(pt, s, o2):
        return CDMISS

    a1 = -173.4292
    a2 = 249.6339
    a3 = 143.3483
    a4 = -21.8492

    b = (-0.033096, 0.014259, -0.0017)

    pt += 273.15
    ptpc = pt / 100.0
    o2c = exp(a1 + a2 * (100.0 / pt) + a3 * log(ptpc) + a4 * ptpc + 
              s * polynomial(ptpc, b))

    return (o2 / o2c) * 100.0


def salinity(press, potemp, sig):
    '''Calculate salinity using international equation of state and back 
       calculating the sigma theta portion of sigma_r().
 
       From text furnished by J. Gieskes
 
       Args:
           press  -- pressure in decibars
           potemp -- potential temperature in celsius degrees
           sig    -- kg/m*3 - 1000.0

       Return:
           sal    -- salinity PSS 78
    '''
    if _any_missing(press, sig, pt):
        return CDMISS

    # Calculate density at given salinity, temp and pressure=0.0
    # First approximation
    salty = 34.5
    rhow = rho_w(potemp)
    kw_ = kw(potemp, salty)

    for i in range(1, 20):
        # get derivative of kw with respect to salinity
        dkwdsl = kw_ / salty + 0.824493

        kst0 = k_st0(salty, potemp)

        # get derivative of kst0
        dkst0 = kst0 / abs(salty) ** 1.5 * 1.5 * sqrt(salty)

        # sigma theta kg/m**3
        sigma = rhow + kw_ + kst0 + 4.8314e-4 * salty ** 2

        # get derivative of sigma theta
        dsig0 = dkwdsl + dkst0 + 9.6628e-4 * salty

        # get next approximation to salinity
        sigma -= 1000.0
        f = sigma - sig
        dfdt = dsig0
        salnew = salty - f / dfdt

        if abs(salnew) < 2000.0:
            # guess isn't too ridiculous.
            if equal_with_epsilon(salnew, salty, TOLER):
                # succesive sal. approximations are very close. stop.
                return salnew
            else:
                salty = salnew
        else:
            print ' Numerical problem in salinity. Setting sal to missing.'
            return CDMISS

    if abs(f) < TOLER:
        # resultant sigma is close enough.
        sal = (salty + salnew) / 2.0
        print ' averaging sal.=%f' % sal
        return sal
    else:
        return CDMISS


def salinity_back_from_in_situ_density(temp, salty, press, stpned):
    '''Back calculate salinity using in situ density.
 
       Args:
           temp      in situ temperature.
           salty     a first guess at salinity.
           press     pressure in decibars.
           stpned    the sigma stp at the point in question
                       calculated as:
                       call sigma_r(p,p,t,s,stp)

       Return:
           sal       the resultant back calculated salinity.
    '''
    # Check for missing parameters.
    if _any_missing(temp, salty, stpned, press):
        return CDMISS

    plus = 0.04

    for i in range(1, 20):
        # Calculate STP at the guess salinity
        stpcal = sigma_r(press, press, temp, salty)
    
        # Add a little bit and calculate it again.
        salx = salty + plus
        stpx = sigma_r(press,press,temp,salx)
    
        # Figure the gradient.
        grad = plus / (stpx - stpcal)
    
        # Figure how much to add to the guessed salinity.
        saladd = grad * (stpned - stpcal)
        salnew = salty + saladd
    
        if abs(salnew) < 1000.0:
            # Guess isn't too ridiculous.
            if equal_with_epsilon(salnew, salty, TOLER):
                # succesive sal. approximations are very close. stop.
                return salnew
            else:
                salty = salnew
        else:
            print (' Numerical problem in salinity_back_from_in_situ_density. '
                   'Setting sal to missing.')
            return CDMISS

    if equal_with_epsilon(stpned, stpcal, TOLER):
        # resultant sigma is close enough.
        sal = (salty + salnew) / 2.0
        print ' averaging sal.=%f' % sal
        return sal
    else:
        return CDMISS
