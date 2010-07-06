""" Test case for libcchdo.formats.bottle.exchange """

import libcchdo
import StringIO # Unit test loading considers the file a module and tries to load it if using from module import.
import libcchdo.formats.bottle.exchange as exchangebot
from unittest import TestCase

class TestBottleExchange(TestCase):
  def setUp(self):
    self.sample = '''BOTTLE,20071011WHPSIODBK
#code : jjward hyd_to_exchange.pl 
#original files copied from HTML directory: 20071009
#original HYD file: i08s_33RR20070204hy.txt   Thu Oct 09 06:52:58 2007
#original SUM file: i08s_33RR20070204su.txt   Wed Oct 08 10:30:46 2007
#
# Parameter:      Lead PI:                  Email Address:
# --------------  ------------------------  ----------------------------
# 13C/14C         Ann McNichol-WHOI         amcnichol@whoi.edu
#                 Robert Key-Princeton      key@Princeton.EDU
#
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DATE,TIME,LATITUDE,LONGITUDE,DEPTH,CTDRAW,CTDPRS,CTDTMP,CTDSAL,CTDSAL_FLAG_W,SALNTY,SALNTY_FLAG_W,CTDOXY,CTDOXY_FLAG_W,THETA,OXYGEN,OXYGEN_FLAG_W,SILCAT,SILCAT_FLAG_W,NITRAT,NITRAT_FLAG_W,NITRIT,NITRIT_FLAG_W,PHSPHT,PHSPHT_FLAG_W,CFC-11,CFC-11_FLAG_W,CFC-12,CFC-12_FLAG_W,CFC113,CFC113_FLAG_W,TCARBN,TCARBN_FLAG_W,PCO2,PCO2_FLAG_W,ALKALI,ALKALI_FLAG_W,PCO2TMP
,,,,,,,,,,,,,DBAR,ITS-90,PSS-78,,PSS-78,,UMOL/KG,,DEG C,UMOL/KG,,UMOL/KG,,UMOL/KG,,UMOL/KG,,UMOL/KG,,PMOL/KG,,PMOL/KG,,PMOL/KG,,UMOL/KG,,UATM@T,,UMOL/KG,,DEG_C
  33RR20070204,   I8S,     1,  1,     15,     15,2,20070215,1442,-65.8108,  84.5502,  450,    3.0,      3.0,  -1.1066,  33.4536,2,  33.4640,2,    351.1,2, -1.1066,    352.9,2,    53.64,2,    22.66,2,     0.11,2,     1.58,2,    6.063,2,    3.278,2, -999.000,5,   2121.4,2,   -999.0,9,   2267.2,2, -999.00
  33RR20070204,   I8S,     1,  1,     16,     16,2,20070215,1442,-65.8108,  84.5502,  450,    3.0,      3.0,  -1.1112,  33.4642,2,-999.0000,9,   -999.0,9, -1.1113,   -999.0,9,  -999.00,9,  -999.00,9,  -999.00,9,  -999.00,9,    6.055,2,    3.267,2, -999.000,5,   2121.3,2,   -999.0,9,   2267.0,2, -999.00
  33RR20070204,   I8S,     1,  1,     14,     14,2,20070215,1442,-65.8108,  84.5502,  450,   27.0,     27.0,  -0.9321,  33.8663,2,  33.8736,2,    344.4,2, -0.9328,    343.5,2,    57.82,2,    24.04,2,     0.08,2,     1.72,2,    5.795,2,    3.138,2, -999.000,5,   2153.2,2,   -999.0,9,   2295.6,2, -999.00
  33RR20070204,   I8S,     1,  1,     13,     13,2,20070215,1442,-65.8108,  84.5502,  450,   52.2,     52.2,  -1.4175,  34.0500,2,  34.0653,2,    327.1,2, -1.4187,    326.6,2,    61.02,2,    27.10,2,     0.08,2,     1.94,2,    5.619,2,    2.983,2, -999.000,5,   2184.3,2,   -999.0,9,   2312.8,2, -999.00
  33RR20070204,   I8S,     1,  1,     12,     12,2,20070215,1442,-65.8108,  84.5502,  450,   77.4,     77.4,  -1.7555,  34.2496,2,  34.2529,2,    319.8,2, -1.7571,    318.8,2,    63.61,2,    29.42,2,     0.08,2,     2.06,2,    5.486,2,    2.914,2, -999.000,5,   2206.2,2,   -999.0,9,   2315.8,2, -999.00
  33RR20070204,   I8S,     1,  1,     11,     11,2,20070215,1442,-65.8108,  84.5502,  450,  102.6,    102.6,  -1.7821,  34.2861,2,  34.2938,4,    319.6,2, -1.7841,    319.1,2,    63.80,2,    29.67,2,     0.08,2,     2.07,2,    5.508,2,    2.910,2, -999.000,5,   2209.2,2,   -999.0,9,   2318.4,2, -999.00
  33RR20070204,   I8S,     1,  1,     10,     10,2,20070215,1442,-65.8108,  84.5502,  450,  152.7,    152.7,  -1.7672,  34.3400,2,-999.0000,5,    315.5,2, -1.7704,    318.9,2,    64.40,2,    30.01,2,     0.06,2,     2.07,2,    5.487,2,    2.906,2, -999.000,5,   2213.3,2,   -999.0,9,   2320.4,2, -999.00
  33RR20070204,   I8S,     1,  1,      9,     09,2,20070215,1442,-65.8108,  84.5502,  450,  203.0,    203.0,  -1.8199,  34.3554,2,  34.3580,2,    320.6,2, -1.8241,    322.4,2,    63.80,2,    29.95,2,     0.03,2,     2.06,2,    5.683,2,    2.987,2, -999.000,5,   2213.5,2,   -999.0,9,   2321.5,2, -999.00
END_DATA
'''
    self.output = '''BOTTLE,20071011WHPSIODBK
#code : jjward hyd_to_exchange.pl 
#original files copied from HTML directory: 20071009
#original HYD file: i08s_33RR20070204hy.txt   Thu Oct 09 06:52:58 2007
#original SUM file: i08s_33RR20070204su.txt   Wed Oct 08 10:30:46 2007
#
# Parameter:      Lead PI:                  Email Address:
# --------------  ------------------------  ----------------------------
# 13C/14C         Ann McNichol-WHOI         amcnichol@whoi.edu
#                 Robert Key-Princeton      key@Princeton.EDU
#
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DATE,TIME,LATITUDE,LONGITUDE,DEPTH,CTDRAW,CTDPRS,CTDTMP,CTDSAL,CTDSAL_FLAG_W,SALNTY,SALNTY_FLAG_W,CTDOXY,CTDOXY_FLAG_W,THETA,OXYGEN,OXYGEN_FLAG_W,SILCAT,SILCAT_FLAG_W,NITRAT,NITRAT_FLAG_W,NITRIT,NITRIT_FLAG_W,PHSPHT,PHSPHT_FLAG_W,CFC-11,CFC-11_FLAG_W,CFC-12,CFC-12_FLAG_W,CFC113,CFC113_FLAG_W,TCARBN,TCARBN_FLAG_W,PCO2,PCO2_FLAG_W,ALKALI,ALKALI_FLAG_W,PCO2TMP
,,,,,,,,,,,,,DBAR,ITS-90,PSS-78,,PSS-78,,UMOL/KG,,DEG C,UMOL/KG,,UMOL/KG,,UMOL/KG,,UMOL/KG,,UMOL/KG,,PMOL/KG,,PMOL/KG,,PMOL/KG,,UMOL/KG,,UATM@T,,UMOL/KG,,DEG_C
  33RR20070204,   I8S,     1,  1,     15,     15,2,20070215,1442,-65.8108,  84.5502,  450,    3.0,      3.0,  -1.1066,  33.4536,2,  33.4640,2,    351.1,2, -1.1066,    352.9,2,    53.64,2,    22.66,2,     0.11,2,     1.58,2,    6.063,2,    3.278,2, -999.000,5,   2121.4,2,   -999.0,9,   2267.2,2, -999.00
  33RR20070204,   I8S,     1,  1,     16,     16,2,20070215,1442,-65.8108,  84.5502,  450,    3.0,      3.0,  -1.1112,  33.4642,2,-999.0000,9,   -999.0,9, -1.1113,   -999.0,9,  -999.00,9,  -999.00,9,  -999.00,9,  -999.00,9,    6.055,2,    3.267,2, -999.000,5,   2121.3,2,   -999.0,9,   2267.0,2, -999.00
  33RR20070204,   I8S,     1,  1,     14,     14,2,20070215,1442,-65.8108,  84.5502,  450,   27.0,     27.0,  -0.9321,  33.8663,2,  33.8736,2,    344.4,2, -0.9328,    343.5,2,    57.82,2,    24.04,2,     0.08,2,     1.72,2,    5.795,2,    3.138,2, -999.000,5,   2153.2,2,   -999.0,9,   2295.6,2, -999.00
  33RR20070204,   I8S,     1,  1,     13,     13,2,20070215,1442,-65.8108,  84.5502,  450,   52.2,     52.2,  -1.4175,  34.0500,2,  34.0653,2,    327.1,2, -1.4187,    326.6,2,    61.02,2,    27.10,2,     0.08,2,     1.94,2,    5.619,2,    2.983,2, -999.000,5,   2184.3,2,   -999.0,9,   2312.8,2, -999.00
  33RR20070204,   I8S,     1,  1,     12,     12,2,20070215,1442,-65.8108,  84.5502,  450,   77.4,     77.4,  -1.7555,  34.2496,2,  34.2529,2,    319.8,2, -1.7571,    318.8,2,    63.61,2,    29.42,2,     0.08,2,     2.06,2,    5.486,2,    2.914,2, -999.000,5,   2206.2,2,   -999.0,9,   2315.8,2, -999.00
  33RR20070204,   I8S,     1,  1,     11,     11,2,20070215,1442,-65.8108,  84.5502,  450,  102.6,    102.6,  -1.7821,  34.2861,2,  34.2938,4,    319.6,2, -1.7841,    319.1,2,    63.80,2,    29.67,2,     0.08,2,     2.07,2,    5.508,2,    2.910,2, -999.000,5,   2209.2,2,   -999.0,9,   2318.4,2, -999.00
  33RR20070204,   I8S,     1,  1,     10,     10,2,20070215,1442,-65.8108,  84.5502,  450,  152.7,    152.7,  -1.7672,  34.3400,2,-999.0000,5,    315.5,2, -1.7704,    318.9,2,    64.40,2,    30.01,2,     0.06,2,     2.07,2,    5.487,2,    2.906,2, -999.000,5,   2213.3,2,   -999.0,9,   2320.4,2, -999.00
  33RR20070204,   I8S,     1,  1,      9,     09,2,20070215,1442,-65.8108,  84.5502,  450,  203.0,    203.0,  -1.8199,  34.3554,2,  34.3580,2,    320.6,2, -1.8241,    322.4,2,    63.80,2,    29.95,2,     0.03,2,     2.06,2,    5.683,2,    2.987,2, -999.000,5,   2213.5,2,   -999.0,9,   2321.5,2, -999.00
END_DATA
'''

  def test_read(self):
    self.file = libcchdo.DataFile()
    self.buff = StringIO.StringIO(self.sample)
    exchangebot.read(self.file, self.buff)
    self.buff.close()

  def test_write(self):
    #self.file = libcchdo.DataFile()
    self.buff = StringIO.StringIO()
    self.assertRaises(NotImplementedError,
            exchangebot.write, self.file, self.buff)
    #exchangebot.write(self.buff)
    #self.assertEqual(self.buff.getvalue(), self.output)
    self.buff.close()
