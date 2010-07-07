import StringIO
import libcchdo
import libcchdo.formats.ctd.woce as wctd
import unittest

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
"""
        self.expected_output = """\
EXPOCODE 99XX19800101   WHP-ID XX00  DATE 010180
STNNBR 42       CASTNO 42  NO. RECORDS=5    
INSTRUMENT NO. 0     SAMPLING RATE 42.00  HZ
  CTDPRS  CTDTMP  CTDSAL  CTDOXY  NUMBER QUALT1
    DBAR  ITS-90  PSS-78 UMOL/KG    OBS.       
 ******* ******* ******* *******               
"""#     3.0 28.7977 31.8503   209.5      42   2222

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

