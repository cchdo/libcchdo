from unittest import TestCase
from StringIO import StringIO
from contextlib import closing

from libcchdo.model.datafile import DataFile
from libcchdo.formats.ctd import exchange as ctdex
from libcchdo.fns import _decimal


class TestCTDExchange(TestCase):

    def test_functional_write(self):
        dfile = DataFile()
        dfile.create_columns(['CTDPRS', 'CTDOXY'])
        dfile['CTDPRS'].parameter.display_order = 0
        dfile['CTDOXY'].parameter.display_order = 1
        dfile['CTDPRS'].values = map(_decimal, ['2.0', '4.0'])
        dfile['CTDOXY'].values = map(_decimal, ['254.0', '253.1'])
        dfile['CTDOXY'].flags_woce = [2, 3]

        with closing(StringIO()) as buff:
            ctdex.write(dfile, buff)
            result = buff.getvalue().split('\n')
            self.assertEqual([
                u'        2.0', u'      254.0', u'2'], result[4].split(','))
