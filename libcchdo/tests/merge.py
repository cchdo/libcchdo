"""Test cases for merge.

"""
from unittest import TestCase
from logging import StreamHandler
from tempfile import TemporaryFile

from libcchdo.fns import _decimal
from libcchdo.model.datafile import DataFile
from libcchdo.merge import Merger, different_columns
from libcchdo.log import LOG

from libcchdo.formats.ctd import exchange as ctdex


class TestMerge(TestCase):
    def test_different_columns(self):
        with TemporaryFile() as origin, TemporaryFile() as deriv:
            origin.write("""\
CTD,20120515ODF
# REPORTED CAST DEPTH IS CTD_DEPTH + DISTANCE_ABOVE_BOTTOM AT MAX PRESSURE
NUMBER_HEADERS = 11
EXPOCODE = 33AT20120419
SECT_ID = A20
STNNBR = 1
CASTNO = 1
DATE = 20120421
TIME = 1552
LATITUDE =   6.8682
LONGITUDE =  -53.4793
DEPTH =    66
INSTRUMENT_ID = 796
CTDPRS,CTDPRS_FLAG_W,CTDTMP,CTDTMP_FLAG_W,CTDSAL,CTDSAL_FLAG_W,CTDOXY,CTDOXY_FLAG_W,CTDNOBS,CTDETIME
DBAR,,ITS-90,,PSS-78,,UMOL/KG,,,
      0.0,6,  27.7514,6,  31.2862,6,    229.5,6,        1,    629.9
      2.0,2,  27.7223,2,  31.3925,2,    229.5,2,       11,    640.0
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
CTD,20120515ODF
# REPORTED CAST DEPTH IS CTD_DEPTH + DISTANCE_ABOVE_BOTTOM AT MAX PRESSURE
NUMBER_HEADERS = 11
EXPOCODE = 33AT20120419
SECT_ID = A20
STNNBR = 1
CASTNO = 1
DATE = 20120421
TIME = 1552
LATITUDE =   6.8682
LONGITUDE =  -53.4793
DEPTH =    66
INSTRUMENT_ID = 796
CTDPRS,CTDPRS_FLAG_W,CTDTMP,CTDTMP_FLAG_W,CTDSAL,CTDSAL_FLAG_W,CTDOXY,CTDOXY_FLAG_W,TRANSM,TRANSM_FLAG_W,CTDNOBS,CTDETIME
DBAR,,ITS-90,,PSS-78,,UMOL/KG,,0-5VDC,,,
      0.0,6,  27.7514,6,  31.2862,2,    222.2,6,   4.3348,1,        1,    629.9
      2.0,2,  27.7223,2,  31.3925,2,    229.5,2,   4.3334,1,       11,    640.0
""")
            deriv.flush()
            deriv.seek(0)

            dforigin = DataFile()
            dfderiv = DataFile()
            ctdex.read(dforigin, origin)
            ctdex.read(dfderiv, deriv)
            self.assertEqual(
                (['CTDSAL', 'CTDOXY'], ['TRANSM']),
                different_columns(dforigin, dfderiv))

    def test_integration_merge_btl(self):
        with    TemporaryFile() as origin, \
                TemporaryFile() as deriv, \
                TemporaryFile() as logstream:
            origin.write("""\
BOTTLE,19700101CCHSIOXXX
# header 1
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 36, 36,2,1000,5,-999.000,9
 316N145_9, TRNS1, 574, 1, 35, 35,2,1000,5,-999.000,9
 316N145_9, TRNS1, 574, 1, 34, 34,2,1000,5,-999.000,9
 316N145_9, TRNS1, 574, 1, 32, 32,2,1000,5,-999.000,9
END_DATA
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
BOTTLE,19700101CCHSIOXXX
# header 2
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 36, 36,2,1000,5,  10.000,9
 316N145_9, TRNS1, 574, 1, 35, 35,2,1000,5,-999.000,1
 316N145_9, TRNS1, 574, 1, 34, 34,2,1000,5,-999.000,9
 316N145_9, TRNS1, 600, 1,  1,  1,2,1000,5,-999.000,9
END_DATA
""")
            deriv.flush()
            deriv.seek(0)

            loghandler = StreamHandler(logstream)
            LOG.addHandler(loghandler)

            merger = Merger(origin, deriv)
            parameters = merger.different_cols()
            mdata = merger.merge(parameters)
            dfile = mdata.convert_to_datafile(parameters)

            self.assertEqual(dfile['DELC14'][0], _decimal('10.000'))
            self.assertEqual(dfile['DELC14'].flags_woce[1], 1)

            # Header should be the origin file's header
            self.assertNotIn('header 2', dfile.globals['header'])
            self.assertIn('header 1', dfile.globals['header'])
            # Header should contain the merged parameters
            self.assertIn('Merged parameters: DELC14, DELC14_FLAG_W', dfile.globals['header'])

            # Key columns should not have been converted to floats. This happens
            # for some reason if pandas combine/update have been used.
            self.assertEqual(str(dfile['STNNBR'][0]), '574')
            self.assertEqual(str(dfile['CASTNO'][0]), '1')
            self.assertEqual(str(dfile['SAMPNO'][0]), '36')
            self.assertEqual(str(dfile['BTLNBR'][0]), '36')

            # Extra keys in derivative file should not be merged in.
            self.assertNotIn(600, dfile['STNNBR'])

            # Make sure warning is printed regarding extra key in deriv file.
            logstream.seek(0)
            found = False
            for line in logstream:
                if (    '600' in line and
                        'in derivative file is not in origin' in line):
                    found = True
                    break
            self.assertTrue(found)
            LOG.removeHandler(loghandler)

    def test_merge_btl_no_common_keys(self):
        """Warn if there are no common keys."""
        with    TemporaryFile() as origin, \
                TemporaryFile() as deriv, \
                TemporaryFile() as logstream:
            origin.write("""\
BOTTLE,19700101CCHSIOXXX
# header 1
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 16, 36,2,1000,5,-999.000,9
 316N145_9, TRNS1, 574, 1, 15, 35,2,1000,5,-999.000,9
END_DATA
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
BOTTLE,19700101CCHSIOXXX
# header 2
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 36, 36,2,1000,5,  10.000,9
 316N145_9, TRNS1, 574, 1, 35, 35,2,1000,5,-999.000,1
END_DATA
""")
            deriv.flush()
            deriv.seek(0)

            loghandler = StreamHandler(logstream)
            LOG.addHandler(loghandler)

            merger = Merger(origin, deriv)
            parameters = merger.different_cols()
            mdata = merger.merge(parameters)
            self.assertEqual(mdata, None)

            # Make sure warning is printed regarding extra key in deriv file.
            logstream.seek(0)
            found = False
            for line in logstream:
                if 'No keys matched' in line:
                    found = True
                    break
            self.assertTrue(found)
            LOG.removeHandler(loghandler)
