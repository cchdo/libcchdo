'''
                           Program cvuwoce
    Converts historical units in WOCE format hydro data to /KG units.
 
 
   A small percentage of WOCE format hydro data is submitted with oxygens
 (both bottle and CTD) in ML/L and with nutrients in UMOL/L.  This program
 will detect and convert data in /L units and convert them to /KG units.
 
  Usage:   cvuwoce
 
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

David Newton 4Mar1999

Port to Python from Fortran 77 Matt Shen 2010-07-26
'''


import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))


import libcchdo.algorithms.volume


def main():
    '''Program cvuwoce converts ML/L and UMOL/L units in WOCE format data
       to /KG.
    '''
    
    print 'cvuwoce converts WOCE format L/L units to /KG.'
    print 'enter input (.sea) filename:'
    
    filename = sys.stdin.readlines()
    file = open(filename, 'r')

    # read and save top line
    head1 = file.readline()
    if not head1.startswith('EXPOCODE'):
        print (' 1st line contains no EXPOCODE string.\n'
               ' This is not WOCE format data. aborting.')
        return 1

    # read second line
    head2 = file.readline()
    eol = head2.index('QUALT1')
    if not eol:
        print (' 2nd line contains no QUALT1 string.\n '
               ' This is not WOCE format data or the lines are too long. '
               'aborting.')
        return 1

    eol += 5

    # search backward on line for non-blank.
    for i in range(eol - 6, 1, -1):
        if head2[i] != ' ':
            # found last column used by data. EndOfParameters
            eop = i
            if eop % 8 != 0:
                print ('Looks like data names are not on 8 character '
                       'boundaries. aborting.')
                return 1

    i = head.index('QUALT2')
    if i:
        # found a QUALT2. this is the real eol.
        eol = i + 5

    # Convinced this is WOCE data. now ask for output filename.
    print 'enter output filename:'
    outfilename = sys.stdin.readline()
    outfile = open(outfilename, 'w')

    outfile.write(head1)
    outfile.write(head2)

    head3 = file.readline()
    num_to_convert = 0
    yesoxy = False
    maxdat = 30

    for k in range(1, maxdat):
        i = (k - 1) * 8 + 1
        j = i + 7
        if i > eop:
            # have reached end of data on line.
            break
        if head3[i:j].index('L/L'):
            # have found unit with L/L in it. Add to Number To Convert.
            num_to_convert += 1
            ctc[num_to_convert] = i
            if head3[i + 6] != '/':
                print head2[i:j], 'unit label not properly placed. aborting.'
                return 1
            print ' %8s %8s' % (head2[i:j], head3[i:j])
            if head2[i:j].index('OXY'):
                if head2[i:j].index('CTDOXY'):
                    # this row of oxygen is CTD
                else:
                    # this row of oxygens is bottle. set flag
                    yesoxy = True
                ndp[num_to_convert] = 1
            elif head2[i:j].index('SIL'):
                ndp[num_to_convert] = 2
            elif head2[i:j].index('NITRA'):
                ndp[num_to_convert] = 2
            elif head2[i:j].index('NITRI'):
                ndp[num_to_convert] = 2
            elif head2[i:j].index('NO2+N'):
                ndp[num_to_convert] = 2
            elif head2[i:j].index('PHSPH'):
                ndp[num_to_convert] = 2
            else:
                print (head2[i:j], 'unexpected conversion.\n How many points '
                       'past decimal in output?:')
                ndp[num_to_convert] = int(sys.stdin.readline())

    if num_to_convert == 0:
        print ' No units to change found!\n Output file is incomplete.'
        return 0
    else:
        print 'Found %d data columns to convert' % num_to_convert
        print '  Bottle oxygen was %s one of them.' % ('' if yexoxy or 'NOT')
        print ' <enter> to continue; Q to quit:'
        ch1 = sys.stdin.readline()
        if ch1 in ['q', 'Q']:
            print ' Program quit.'
            return 0

    # Fix the units line and print it out.
    for k in range(1, num_to_convert):
        i = ctc[k]
        j = i + 7
        if head2[i:j].index('OXY'):
            head3[i:j] = ' UMOL/KG'
        else:
            # need a U,P, or N in MOLS string.
            c8 = ' ' + head3[i + 2] + 'MOL/KG'
            head3[i:j] = c8
    outfile.write(head3[1:eol])

    # ask about oxygen method.
    if yexoxy:
        print ' Were bottle oxygens Whole bottle or Aliquot? (W/A):'
        ch1 = sys.stdin.readline()
        if ch1 in ('w', 'W'):
            whole = True
        elif ch1 in ('a', 'A'):
            whole = False
            print 'Will use temp=25. for oxygen conversion.'
        else:
            print ' enter W or A.'
            print " In truth it probably doesn't matter."
            # TODO jump back to ch1

    # locate salinity and temperature columns.
    nosal = False
    if head2.index('CTDSAL'):
        isal = head2.index('CTDSAL') - 2
    elif head2.index('SALNTY'):
        isal = head2.index('SALNTY') - 2
    else:
        print ' no salinity found. using 34.8 .'
        nosal = True

    # test position of salinity
    if not nosal:
        if (isal - 1) % 8 != 0:
            print (' salinity or label not on 8 byte boundary.\n fix it. '
                   'aborting.')

    notemp = False
    if head2.index('CTDTMP'):
        itmp = head2.index('CTDTMP') - 2
    elif head2.index('THETA'):
        itmp = head2.index('THETA') - 3
    elif head2.index('REVTMP'):
        itmp = head2.index('REVTMP') - 2
    else:
        print ' no temperature found. using approximations.'
        notemp = True

    # test position of temperature
    if not notemp:
        if (itmp - 1) % 8 != 0:
            print (' temper. or label not on 8 byte boundary.\n fix it. '
                   'aborting.')

    # read and copy silly asterisk line
    outfile.write(file.readline())

    # read all data lines converting one at a time
    nlw = 4

    for line in file:
        if nosal:
            s = 34.8
        else:
            c8 = line[isal:isal + 7]
            try:
                s = float(c8)
            except:
                print ' read err in salinity. aborting. rec#=%d line=%s' % \
                      (nlw + 1, line)
                return 1
            
            if s < 0:
                s = 34.8
            elif s < 20 or s > 60:
                print ' salinity is ridiculous=%f rec#=%d' % (s, nlw + 1)

        # extract temperature
        tmiss = True
        if notemp:
            pass
        else:
            c8 = line[itmp:itmp + 7]
            try:
                t = float(c8)
            except:
                print ' read err in temp. aborting. rec#=%d line=%s' % \
                      (nlw + 1, line)
                return 1
            if t > -3:
                tmiss = False

        for k in range(1, num_to_convert):
            i = ctc[k]
            j = i + 7
            c8 = line[i:j]
            try:
                v = float(c8)
            except:
                print 'unreadable value. aborting. rec#=%d line=%s' % \
                      (nlw + 1, line)
                return 1
            if v < -3:
                # missing
                v = -9.0
            elif head2[i:j].index('OXY'):
                # dealing with oxygen.
                if not whole and not head2[i:j].index('CTDOXY'):
                    # not CTDOXY and yes oxybottles were aliquot.
                    t = 25.0
                elif tmiss:
                    t = 25.0
                    print 'T missing. using 25. at rec#=%d' % nlw + 1
                sigt = libcchdo.algorithms.volume.sigma_p(0, 0, t, s)
                v = v / (0.022392 * (sigt / 1e3 + 1.0))
            else:
                # everything, but oxygen
                pden = libcchdo.algorithms.volume.sigma_p(0, 0, 25.0, s)
                v = v / (pden / 1e3 + 1.0)

            # done converting. print to string.
            if ndp[k] == 2:
                c8 = v % '%8.2f'
            elif ndp[k] == 1:
                c8 = v % '%8.1f'
            elif ndp[k] == 3:
                c8 = v % '%8.3f'
            elif ndp[k] == 4:
                c8 = v % '%8.4f'
            else:
                print " can't print number with %d dec point." % ndp[k]
                return 1
            line[i:j] = c8

        outfile.write(line)
        nlw += 1

    # get here on normal end of file.
    print '%d lines written.' % nlw
    file.close()
    outfile.close()


if __name__ == '__main__':
    sys.exit(main())
