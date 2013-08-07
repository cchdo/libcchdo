"""Test cases for merge.

"""
from unittest import TestCase
from logging import StreamHandler
from tempfile import TemporaryFile

from libcchdo.fns import _decimal
from libcchdo.model.datafile import DataFile
from libcchdo.merge import Merger
from libcchdo.log import LOG


class TestMerge(TestCase):
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
