import StringIO
import libcchdo
import libcchdo.formats.ctd.woce as wctd
import unittest
import sys

class TestCTDWoceAlpha (unittest.TestCase):
    def setUp(self):
        self.input = """\
EXPOCODE 99XX19800101   WHP-ID XX00  DATE 010180
STNNBR 42       CASTNO 42  NO. Records=5 
INSTRUMENT NO. 0     SAMPLING RATE 42.00  HZ
  CTDPRS  CTDTMP  CTDSAL  CTDOXY  NUMBER QUALT1
    DBAR  ITS-90  PSS-78 UMOL/KG    OBS.      *
 ******* ******* ******* *******              *
     3.0 28.7977 31.8503   209.5      42   2222
     5.0 28.7978 32.0889   208.6       9   2333
     7.0 28.7995 32.3976   210.8      41   2222
     9.0 28.8014 33.0838   212.1      64   2222
    11.0 28.8018 34.6452   199.5     630   2346
    13.0 28.8018 34.4182   198.2      42   2222
    15.0 28.8018 34.4240   202.1      26   2222
    17.0 28.7814 34.4247   202.6      36   2222
    19.0 28.7541 34.4258   199.1      16   2222
    21.0 28.6938 34.4247   190.8     255   2226
    23.0 28.6380 34.4163   193.0      96   2222
    25.0 28.6206 34.4135   193.0      27   2222
    27.0 28.5760 34.4431   197.8      21   2222
    29.0 28.3347 34.5718   203.4      60   2232
    31.0 28.2823 34.5917   197.3     133   2336
    33.0 28.2182 34.5822   199.1      81   2222
    35.0 28.1500 34.6755   203.4     105   2246
    37.0 28.1233 34.5777   201.7      34   2222
"""
        self.expected_output = """\
EXPOCODE 99XX19800101   WHP-ID XX00  DATE 010180
STNNBR 42       CASTNO 42  NO. RECORDS=5    
INSTRUMENT NO. 0     SAMPLING RATE 42.00  HZ
  CTDPRS  CTDTMP  CTDOXY  CTDSAL CTDNOBS QUALT1
    DBAR  ITS-90 UMOL/KG  PSS-78               
 ******* ******* ******* *******               
     3.0 28.7977   209.5 31.8503    42.0   2222
     5.0 28.7978   208.6 32.0889     9.0   2333
     7.0 28.7995   210.8 32.3976    41.0   2222
     9.0 28.8014   212.1 33.0838    64.0   2222
    11.0 28.8018   199.5 34.6452   630.0   2364
    13.0 28.8018   198.2 34.4182    42.0   2222
    15.0 28.8018   202.1 34.4240    26.0   2222
    17.0 28.7814   202.6 34.4247    36.0   2222
    19.0 28.7541   199.1 34.4258    16.0   2222
    21.0 28.6938   190.8 34.4247   255.0   2262
    23.0 28.6380   193.0 34.4163    96.0   2222
    25.0 28.6206   193.0 34.4135    27.0   2222
    27.0 28.5760   197.8 34.4431    21.0   2222
    29.0 28.3347   203.4 34.5718    60.0   2223
    31.0 28.2823   197.3 34.5917   133.0   2363
    33.0 28.2182   199.1 34.5822    81.0   2222
    35.0 28.1500   203.4 34.6755   105.0   2264
    37.0 28.1233   201.7 34.5777    34.0   2222
"""

    def test_read(self):
        self.file = libcchdo.DataFile()
        self.bufr = StringIO.StringIO(self.input)
        wctd.read(self.file, self.bufr)
        self.bufr.close()

    def test_read_write(self):
        self.file = libcchdo.DataFile()
        self.bufr = StringIO.StringIO(self.input)
        wctd.read(self.file, self.bufr)
        self.bufr.close()
        self.bufr = StringIO.StringIO()
        wctd.write(self.file, self.bufr)
        self.assertEqual(self.expected_output, self.bufr.getvalue())
        self.bufr.close()

